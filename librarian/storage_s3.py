#!/usr/bin/env python
# upload-s3.py -- Librarian script that takes care of uploading data to AWS S3

import boto
import boto.s3.connection
import os
import datetime


class StorageS3:
  ''' Manages the storage and retrieval of files from S3 '''
  def __init__(self, secret_access_key=None, access_key_id=None, bucket='librarian_upload_test'):
    self.secret_key = secret_access_key
    self.access_key = access_key_id
    self.bucketname = bucket
    try:
      self.conn = boto.s3.connection.S3Connection(
        aws_access_key_id = self.access_key,
        aws_secret_access_key = self.secret_key,
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )
    except:
      raise Exception('Invalid S3 credentials')
      
  def _list_files(self, directory):
    ''' Generator to recursively list all the files in a directory. '''
    if os.path.isfile(directory):
      yield directory
      raise StopIteration
    for f in os.listdir(directory):
      name = directory + '/' + f
      if os.path.isfile(name):
        yield name
      elif os.path.isdir(name):
        for f in self._list_files(name):
          yield f


  def upload(self, local_path, project, dataset, timestamp):
    ''' Upload a file/directory to S3 path maintaining the directory
      structure
    '''
    # get the storage bucket
    bucket = self.conn.get_bucket(self.bucketname)
  
    # make a separate directory for the installation
    store_dir = '/'.join([project, dataset, timestamp])
    urls = []
    checksums = []
  
    # Upload all the files and directories pointed to by local_path
    for f in self._list_files(local_path):
      key = bucket.new_key(store_dir + f[len(local_path):])
      key.set_contents_from_filename(f)
      key.set_acl('public-read')
      url = key.generate_url(expires_in=0, query_auth=False)
      urls.append(url)
      checksums.append('') # TODO
    # return the urls and checksums for uploaded objects
    return urls, checksums
  
