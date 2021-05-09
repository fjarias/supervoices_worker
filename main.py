# pip install pytransloadit
import os
from transloadit import *
import requests
import boto3
import json
import time
from signal import signal, SIGINT, SIGTERM


# Change parameter backend endpoint
def getBackendEndpoint():
    return 'http://3.142.39.92:7001/api/daemon/voices'


# Change parameter SQS provider (AWS)
def getSqsUrl():
    return 'https://sqs.us-east-1.amazonaws.com/978716278222/Super-Voices-MQ'


# Change parameter emailing backend endpoint
def getEmailingEndpoint():
    return 'https://script.google.com/macros/s/AKfycbxj2E2WGphd142e8c7koHEVnBRtngNE2DpceaYYP9biEOmPsJSZPxfk/exec'


# Set status for a voice list
def mark_voice(file_name_out, voice_id, converted_duration):
    obj = {
        'converted_duration': converted_duration,
        'status': 'Converted',
        'file_path_final': 'voices_out/' + file_name_out,
        'voice_length': 0
    }
    url = getBackendEndpoint() + '/' + voice_id
    try:
        requests.put(url, json=obj)
    except Exception as e:
        print(f"exception while marking the voice: {repr(e)}")
        return -1
    return 0


# Handle for gracefully termination
class SignalHandler:
    def __init__(self):
        self.received_signal = False
        signal(SIGINT, self._signal_handler)
        signal(SIGTERM, self._signal_handler)

    def _signal_handler(self, signal, frame):
        print(f"handling signal {signal}, exiting gracefully")
        self.received_signal = True


if __name__ == '__main__':
    sqs = boto3.client('sqs', region_name='us-east-1')

    signal_handler = SignalHandler()
    while not signal_handler.received_signal:

        response = sqs.receive_message(
            QueueUrl=getSqsUrl(),
            MaxNumberOfMessages=1,
            WaitTimeSeconds=2
        )

        # Check if messages in the queue
        if not ('Messages' in response):
            continue

        for i in range(len(response['Messages'])):
            start_time = time.time()
            try:
                receipt_handle = response['Messages'][i]['ReceiptHandle']
                voice = json.loads(str(response['Messages'][i]['Body']))
                voice_id = voice['id']
                file_extension = voice['file_name'][len(voice['file_name']) - 4:len(voice['file_name'])]
                file_name = str(voice_id) + file_extension
                file_name_out = file_name[0:len(file_name) - 4] + '.mp3'
                locutor_name = voice['locutor_name'] + ' ' + voice['locutor_lastname']
                locutor_email = voice['locutor_email']
                title = voice['title']

                tl_key = os.environ['TL_KEY']
                tl_secret = os.environ['TL_SECRET']

                tl = client.Transloadit(tl_key, tl_secret)
                assembly = tl.new_assembly()

                # Conversion process
                # Set Encoding Instructions
                assembly.add_step('import', '/s3/import', {
                    'path': 'voices_in/' + file_name,
                    'credentials': 'aws_credentials'
                })
                assembly.add_step('encode_mp3', '/audio/encode', {
                    'use': 'import',
                    'preset': 'mp3',
                    'ffmpeg_stack': 'v3.3.3'
                })
                assembly.add_step('export', '/s3/store', {
                    'use': [
                        'encode_mp3'
                    ],
                    'path': 'voices_out/' + file_name,
                    'credentials': 'aws_credentials'
                })

                # Conversion by Transloadit
                assembly_response = assembly.create(retries=5, wait=True)

                # PUT command to set voice status in 'Converted'
                mark_voice(file_name_out, voice_id, time.time() - start_time)
            except Exception as e:
                print(f"exception while processing message: {repr(e)}")
                continue

            # Delete SQS message
            response = sqs.delete_message(
                QueueUrl=getSqsUrl(),
                ReceiptHandle=receipt_handle
            )

            # Send notification email
            subject = 'SuperVoices: Tu voz ha sido procesada!'
            url = getEmailingEndpoint()
            my_query_string = {'mail_to': locutor_email,
                               'mail_subject': subject,
                               'recipient_name': locutor_name,
                               'title': title
                               }
            r = requests.get(url, params=my_query_string)
            print("se envió un email a " + locutor_email)
