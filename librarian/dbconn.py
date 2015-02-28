#!/bin/python
"""Database connectivity for Librarian.

This module contains all the classes and miscellany necessary for
Librarian to connect to a shared backend RDBMS for metadata.  It
is not designed to hold raw content, just the file names, version
history, checksums, etc.

Schema:
'Engagements': ['id', 'name', 'date_started', 'owner', 'comments'], 

'IncomingData': ['id', 'project', 'name', 'version', 'timestamp', 
      'urls', 'checksums', 'metadata_url', 'comments', 'username', 'hostname']

'OutgoingData': ['id', 'project', 'name', 'version', 'timestamp', 
      'urls', 'checksums', 'metadata_url', 'comments', 'username', 'hostname']

"""

import MySQLdb, datetime, urllib
Schema = {
'Engagements': ['id', 'name', 'date_started', 'owner', 'comments'], 

'IncomingData': ['id', 'project', 'name', 'version', 'timestamp', 
      'urls', 'checksums', 'metadata_url', 'comments', 'username', 'hostname'],

'OutgoingData': ['id', 'project', 'name', 'version', 'timestamp', 
      'urls', 'checksums', 'metadata_url', 'comments', 'username', 'hostname']
}

class DBConn:
  """Represents a live database conn with Librarian-specific operators."""

  def __init__(self, user=None, password=None, host=None, port = 3306):
    self.user = user
    self.pswd = password
    self.host = host
    self.port = int(port)
    try:
        self.db = MySQLdb.connect(host=self.host, port=self.port, user=self.user,
                                    passwd=self.pswd, db='librarian')
    except:
        raise Exception('Invalid credentials for librarian database')
  
  def projectLs(self):
    ''' returns a generator listing all librarian projects '''
    c = self.db.cursor()
    try:
      for _ in xrange(c.execute( '''select name from Engagements''')):
        yield c.fetchone()[0]
    except Exception:
      raise Exception('Database not available')
    c.close()
    
  def ls(self, project):
    ''' returns a generator listing all datasets and their versions in
        a librarian project. The generator yield a (name, version) tuple.
    '''
    c = self.db.cursor()

    def datasetQueryFor(table):
        return '''
        select ds.name, ds.version, ds.timestamp
          from %s ds
          join Engagements on ds.project = Engagements.id
         where Engagements.name = %%s
        ''' % (table)
    try:
      for _ in xrange(c.execute(datasetQueryFor('IncomingData') +
                                ' union all ' + datasetQueryFor('OutgoingData'),
                                (project, project))):
        yield c.fetchone()
    except Exception as e:
      raise Exception('Database not available', e)
    c.close()

  def _getTableSignature(self, table):
    ''' A private function to ease query generation during insertion '''
    cols = Schema[table][1:]        # skip the autoincrement field
    return '''insert into %s(%s) values (%s)''' % (table, ','.join(cols), 
                        ','.join(['%s']*len(cols)))

    
  def createProject(self, project, comments=''):
    ''' Creates a new project in the database. This function should be called
        after appropriate space has been allocated on the S3 bucket
    '''
    if project in self.projectLs():
      raise Exception('Project already exists!')
    c = self.db.cursor()
    date = datetime.date.today()
    owner = self.user
    c.execute(self._getTableSignature('Engagements'),
                (project, date, owner, comments))
    self.db.commit()
    c.close()
      
  def addInOrOut(self, project, name, version, timestamp, urls, checksums, 
                    metadata_url, comments, username, hostname, incoming=True):
    ''' Adds info about incoming or outgoing file(s) to librarian database. 
        The incoming or outgoing files must belong to an existing project.
        The timestamp should be a datetime object.
    '''
    
    if project not in self.projectLs():
      raise Exception('Project does not exist!')
    
    # Get the project id
    c = self.db.cursor()
    c.execute('select id from Engagements where name=%s', (project,))
    pid = c.fetchone()[0]
    
    # Add the record to the specified relation
    relation = 'IncomingData' if incoming else 'OutgoingData'
    record = (pid, name, version, timestamp, urls, checksums, metadata_url, 
                comments, username, hostname)
    c.execute(self._getTableSignature(relation), record)
    self.db.commit()
    c.close()
    
  def fetch(self, project, dataset, version, localpath):
    c = self.db.cursor()
    def datasetQueryFor(table):
        return '''
        select ds.urls 
          from %s ds
          join Engagements on ds.project = Engagements.id
          where Engagements.name=%%s and ds.name=%%s and ds.version=%%s
        ''' % (table)
    try:
      for _ in xrange(c.execute(datasetQueryFor('IncomingData') +
                                ' union all ' + datasetQueryFor('OutgoingData'),
                                (project, dataset, version, project, dataset, version))):
        for url in c.fetchone()[0].split():
            # TODO Maintain the directory structure
            ind = url.rindex('/')
            urllib.urlretrieve(url, filename= localpath+url[ind:])
    except Exception:
      raise Exception('Database not available')
    c.close()
    return localpath
    
if __name__=='__main__':
    import json, os, datetime
    cred = json.load(open(os.path.abspath(os.path.expanduser("~/.librarian"))))
    cred = cred['credentials']['mysql']
    conn = DBConn(**cred)
    # list all projects
    projects = [x for x in conn.projectLs()]
    print projects
    # list all files for the first project
    files = [x for x in conn.ls(projects[0])]
    print files
    # testing addInOrOut
    conn.addInOrOut(projects[0], 'test1', '0.0', datetime.datetime.now(), 'urls', 
                        'checksums', 'meta_url', 'some comments', 'dummyuser', 'dummydomain', incoming=False)
                        
    # testing download
    conn = DBConn(**cred)
    conn.fetch('project1', 'test', '0.0', '/home/abhinav/delete')
