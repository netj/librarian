#!/usr/bin/env python
"""Librarian Client Version 0.01

Librarian takes care of all files that leave/enter engagements.  When
a partner provides a new datafile (as with Memex ads), they get added
to Librarian.  When we ship extracted data elsewhere, they get added
to Librarian.

It can also be used to track standard utility files, like Wikipedia or
Freebase dumps.

It should NOT be used to hold temporary or working files.
"""

import argparse, boto, json, os.path, dbconn, storage_s3, datetime

##
# GLOBALS
##
configFilename = os.path.abspath(os.path.expanduser("~/.librarian"))
configDict = {"credentials":{}}

###############################################
class ConfigError(Exception):
  """ConfigError is a basic Exception wrapper class for this application"""
  def __init__(self, msg):
    self.msg = msg


##################################################

def loadConfig():
  """Grab config info from the ondisk file."""
  if not os.path.exists(configFilename):
    raise ConfigError("Librarian config file does not exist.  Invoke librarian --init to create")
    
  configFile = open(configFilename)
  try:
    global configDict
    configDict = json.loads(configFile.read())
  finally:
    configFile.close()


def saveConfig():
  """Save config info to disk file."""
  configFile = open(configFilename, "w")
  try:
    configFile.write(json.dumps(configDict))
  finally:
    configFile.close()


def configInit():
  """Create the local .librarian config file, if none previously existed"""
  if os.path.exists(configFilename):
    raise ConfigError("Cannot init.  Librarian config file already exists at", configFilename)
  saveConfig()


def addCredentials(credentialName, **credential):
  """Add a new credential to the config file for later use"""
  loadConfig()
  global configDict
  configDict["credentials"][credentialName] = credential
  saveConfig()
  
def get_db_access():
  """ get access to the Librarian database """
  loadConfig()
  c = configDict["credentials"]["mysql"]
  db = dbconn.DBConn(c["user"], c["password"], c["host"], c["port"])
  return db

def get_s3_access():
  """ get access to the Librarian s3 storage """
  loadConfig()
  aws_cred = configDict["credentials"]["aws"]
  storage = storage_s3.StorageS3(**aws_cred)
  return storage

def publish(in_or_out, localpath, project, name, version, comment):
  '''Check a file into Librarian. Each project has a different folder in the 
     S3 bucket. Each data submission to a project is stored into a different
     folder corresponding to the time of submission. A submission can be a
     file or a directory. If it is a directory then all its contents are
     recursively stored and the directory structure is maintained.
  '''
  db = get_db_access()
  s3 = get_s3_access()
  if not os.path.exists(localpath):
    raise Exception('Invalid local path specified')
  if project not in db.projectLs():
    print "The given project doesn't exist. Please create it first."
  timestamp = datetime.datetime.now()
  s3_directory = name+timestamp.strftime('-%Y%m%d-%H%M%S%f')
  urls, checksums = s3.upload(localpath, project, name, s3_directory)
  urls = '\n'.join(urls)
  checksums = '\n'.join(checksums)
  metadata_url = ''         #TODO
  username = 'testuser'     #TODO Perhaps store in config
  hostname = 'testhost'     #TODO Perhaps store in config
  incoming = (in_or_out == 'in')
  db.addInOrOut(project, name, version, timestamp, urls, checksums, 
                    metadata_url, comment, username, hostname, incoming=incoming)
  print "File(s) successfully uploaded"
  
  
def get(project, dataset, version, localpath):
  """ Get all files for a dataset version of a specific project 
      and stores it in a new directory located at localpath
  """
  db = get_db_access()
  if project not in db.projectLs():
    print "The given project doesn't exist"
  elif not os.path.isdir(localpath):
    print "The specified local path is not a directory"
  else:
    output = db.fetch(project, dataset, version, localpath)
    print 'Files stored in the directory ', output


def projectLs():
  """List all Librarian projects"""
  db = get_db_access()  
  print "List of all known projects:"
  for name in db.projectLs():
    print name


def ls(projectname):
  """List all Librarian files for a single project"""
  db = get_db_access()
  if projectname not in db.projectLs():
    print "The given project doesn't exist"
  else:
    print "List of all datasets in a project named", projectname
    for name, version, timestamp in db.ls(projectname):
      print '{0:{width}}'.format(name, width=25),
      print '{0:{width}}'.format(version, width=8),
      print timestamp
    
def create(projectname, comment):
  """List all Librarian files for a single project"""
  db = get_db_access()
  if projectname in db.projectLs():
    print "The given project already exists"
  elif comment is None:
    db.createProject(projectname)
  else:
    db.createProject(projectname, comment)
#
# main()
#
def main():
  usage = "usage: %prog [options]"

  # Setup cmdline parsing
  # commands for intializing credentials etc
  parser = argparse.ArgumentParser(description="Librarian stores data")
  parser.add_argument("--version", action="version", version="%(prog)s 0.1")
  parser.add_argument("--init", action="store_true", default=False, 
                        help="Create the initial config file")  
  parser.add_argument("--config", nargs=1, metavar=("configfile"), 
                        help="Location of the Librarian config file")
  parser.add_argument("--lscreds", action="store_true", help="List all known credentials")
  parser.add_argument("--set_aws_cred", nargs=2, metavar=("aws_access_key_id", "aws_secret_access_key"), 
                        help="Stores an AWS credential pair in the Librarian config file")
  parser.add_argument("--set_mysql_cred", nargs=4, 
                        metavar=("mysql_host", "mysql_port", "mysql_user", "mysql_password"), 
                        help="Stores a MySQL connection info quadruple in the Librarian config file")
  
  
  # commands for listing and creating projects
  parser.add_argument("--ls", nargs=1, metavar=("project"), help="List all the files in a <project>")
  parser.add_argument("--pls", action="store_true", default=False, help="List all projects")
  parser.add_argument("--create", nargs=2, metavar=("project", "comment"), 
                        help="Create a new <project> and add a <comment>")
  
  # commands for publishing and fetching data
  parser.add_argument("--publish", nargs=5, metavar=("path", "project", "name", "version", "comment"), 
                        help="Puts contents of <path> into a <project> <version> with a <name> and <comment>")
  parser.add_argument('--type', choices=['in', 'out'], help="Tells whether data is incoming or outgoing")
  parser.add_argument("--get", nargs=4, metavar=("project", "dataset", "version", "dir"), 
                        help="Gets all the files belonging to <project> <dataset> <version> and stores it in directory <dir>")
  
  
  # Invoke either get() or put()
  args = parser.parse_args()
  try:
    if args.config is not None:
      configFilename = os.path.abspath(args.config)
      
    if args.init:
      configInit()
    elif args.set_aws_cred is not None and len(args.set_aws_cred) == 2:
      addCredentials("aws", access_key_id=args.set_aws_cred[0]
                          , secret_access_key=args.set_aws_cred[1]
                          )
    elif args.set_mysql_cred is not None and len(args.set_mysql_cred) == 4:
      addCredentials("mysql", host=args.set_mysql_cred[0]
                            , port=args.set_mysql_cred[1]
                            , user=args.set_mysql_cred[2]
                            , password=args.set_mysql_cred[3]
                            )
    elif args.pls:
      projectLs()
    elif args.ls is not None:
      ls(args.ls[0])
    elif args.create is not None:
      create(args.create[0], args.create[1])
    
    elif args.publish is not None and len(args.publish) > 0:
      if args.type is None:
        print "Please enter the type of data"
      else:
        publish(args.type, *args.publish)
      
    elif args.get is not None and len(args.get) > 0:
      get(*args.get)
    elif args.lscreds:
      loadConfig()
      print "There are", len(configDict["credentials"]), "credential(s) available"
      for name, cred in configDict["credentials"].iteritems():
        # TODO print prettier
        print " ", name, cred
    else:
      parser.print_help()
  except ConfigError as e:
    print e.msg

if __name__ == "__main__":
  main()
