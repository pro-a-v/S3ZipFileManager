'''
    S3ZipFileManager - For AWS ServerLess Projects. When need to parse data from huge zip archives using lambdas in paralel.
    Copyright (C) 2019  Prochakivsky O. V.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
    

'''

# Lambda Handler - lambda_function.s3_bucket_upload_event_handler

import logging
import boto3
import struct
import time
from botocore.vendored.requests import get

''' Log event to CloudWatch '''
logger = logging.getLogger()
logger.setLevel(logging.INFO)

''' Time spending for methods decorator  '''


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print(method.__name__, (te - ts) * 1000, 'ms')
        return result

    return timed


'''
Return info about files inside an archive info
'''


class ZipFile:
    def __init__(self, bucket_name, file_name, sqs_url):
        self.s3 = boto3.client('s3')
        self.chunksize = 256
        self.sqs_url = sqs_url
        self.bucket_name = bucket_name
        self.file_name = file_name
        self.filesize = 0
        self.loaded_size = 0
        self.files = list()
        ''' start download '''

    def get_file_info(self):
        '''  send a HEAD not GET request and get filesize via content-length header '''
        try:
            self.filesize = boto3.resource('s3').Bucket(self.bucket_name).Object(self.file_name).content_length
        except Exception as e:
            print('get_file_info : got Exception')
            print(e)

    def process(self):
        while self.loaded_size < self.filesize - 256:  # In the end of file - 4.3.12  Central directory structure:
            self.get_header()

    @timeit
    def get_header(self):
        '''  If file less than chunksize '''
        if self.filesize < self.chunksize:
            self.chunksize = self.filesize
        try:
            ''' Using requests  '''
            url = "http://stackoverflow.com"
            headers = {'Range': 'bytes={}-{}'.format(self.loaded_size, self.loaded_size + self.chunksize)}
            resp = get('https://s3-eu-west-1.amazonaws.com/' + self.bucket_name + '/' + self.file_name, headers=headers)
            print(resp)
            if resp.status_code == 200 or resp.status_code == 204:
                file_info = ZipHeader(resp.content, self.loaded_size)
                self.loaded_size = self.loaded_size + 30 + file_info.compressed_size + file_info.file_name_length + file_info.extra_field_length
                self.files.append(file_info)
                self.put_to_sqs(file_info.toJSON())

            ''' Using boto3  '''
            '''
            resp = self.s3.get_object(Bucket=self.bucket_name, Key=self.file_name, Range='bytes={}-{}'.format(self.loaded_size, self.loaded_size + self.chunksize))
            file_info = ZipHeader(resp['Body'].read(), self.loaded_size)
            self.loaded_size = self.loaded_size + 30 + file_info.compressed_size + file_info.file_name_length + file_info.extra_field_length
            self.files.append(file_info)
            self.put_to_sqs(file_info.toJSON())
            '''
        except Exception as e:
            print('get_header : got Exception:', e)

    def __repr__(self):
        report_string = ' { "bucket":"' + self.bucket_name + '", "file_name":"' + self.file_name + '", "file-size":' + str(
            self.filesize) + ', "files": ['
        for arch_file in self.files:
            report_string += '  { "name":"' + arch_file.file_name + '",' \
                                                                    ' "compressed_size":' + str(
                arch_file.compressed_size) + ',' \
                                             ' "uncompressed_size":' + str(arch_file.uncompressed_size) + ',' \
                                                                                                          '"data_position_start":' + str(
                arch_file.start_pos + 30 + arch_file.file_name_length + arch_file.extra_field_length) + ',' \
                                                                                                        '"data_position_end":' + str(
                arch_file.start_pos + 30 + arch_file.file_name_length + arch_file.extra_field_length + arch_file.compressed_size) + '' \
                                                                                                                                    ' },'
        report_string = report_string[0:-1] + '] }'
        logger.info(report_string)
        return report_string

    def put_to_sqs(self, file_info):
        sqs = boto3.client('sqs')
        json_string = '{ "bucket":"' + self.bucket_name + '", "zipfilname":"' + self.file_name + '", "data":' + file_info + '}'
        response = sqs.send_message(QueueUrl=self.sqs_url, DelaySeconds=10, MessageBody=(json_string))
        print(response['MessageId'])


'''
https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT

   4.3.6 Overall .ZIP file format:
   4.3.7  Local file header:
   4.3.12  Central directory structure:
   4.3.13 Digital signature:
'''


class ZipHeader:
    def __init__(self, data, position):
        self.local_file_header_signature = 0
        self.version_needed_to_extract = 0
        self.general_purpose_bit_flag = 0
        self.compression_method = 0
        self.last_mod_file_time = 0
        self.last_mod_file_date = 0
        self.crc_32 = ''
        self.compressed_size = 0
        self.uncompressed_size = 0
        self.file_name_length = 0
        self.extra_field_length = 0
        self.file_name = ''
        self.extra_field = ''
        self.start_pos = position

        fields = struct.unpack('<iHHHHHiiiHH', data[0:30])

        self.local_file_header_signature, self.version_needed_to_extract, self.general_purpose_bit_flag, \
        self.compression_method, self.last_mod_file_time, self.last_mod_file_date, self.crc_32, \
        self.compressed_size, self.uncompressed_size, self.file_name_length, self.extra_field_length = fields
        self.file_name = data[30:30 + self.file_name_length].decode()
        self.extra_field = data[30 + self.file_name_length:30 + self.file_name_length + self.extra_field_length]

    def toJSON(self):
        json_string = '{ "file_name": "' + self.file_name + '", "start_pos": ' + str(
            self.start_pos) + ', "uncompressed_size": ' + str(self.uncompressed_size) + ', "compressed_size": ' + str(
            self.compressed_size) + '}'
        return json_string


def s3_bucket_upload_event_handler(event, context):
    logger.info('got event{}'.format(event))

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']
    file_info = str()
    try:
        file_info = ZipFile(bucket_name, file_name,
                            'https://sqs.eu-west-1.amazonaws.com/396835938517/zip_file_metadata_jobs')
        file_info.get_file_info()
        file_info.process()
    except Exception as e:
        print('main: got Exception: ', e)

    return {
        'statusCode': 200,
        'body': print(file_info)
    }




