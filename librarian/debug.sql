/*
Schema:
'Engagements': ['id', 'name', 'date_started', 'owner', 'comments'], 

'IncomingData': ['id', 'project', 'name', 'version', 'timestamp', 
      'urls', 'checksums', 'metadata_url', 'comments', 'username', 'hostname']

'OutgoingData': ['id', 'project', 'name', 'version', 'timestamp', 
      'urls', 'checksums', 'metadata_url', 'comments', 'username', 'hostname']
*/

-- dummy values for testing the database

insert into Engagements(name, date_started, owner, comments) values ('project1', '2014-05-02', 'abhinav', 'project one');
insert into Engagements(name, date_started, owner, comments) values ('project2', '2014-06-01', 'abhinav', 'project two');
insert into Engagements(name, date_started, owner, comments) values ('project3', '2014-08-05', 'abhinav', 'project three');

insert into IncomingData(project, name, version, timestamp, urls, checksums, metadata_url, comments, username, hostname) values (1, 'init', '0.01', '2014-05-03 12:12:12:1234', 'many urls', 'checksum', 'meta_url', 'input 1', 'abhinav', 'stanford');
insert into IncomingData(project, name, version, timestamp, urls, checksums, metadata_url, comments, username, hostname) values (1, 'two', '0.01', '2014-05-03 12:12:12:1234', 'many urls', 'checksum', 'meta_url', 'input 2', 'abhinav', 'stanford');
insert into IncomingData(project, name, version, timestamp, urls, checksums, metadata_url, comments, username, hostname) values (2, 'three', '0.01', '2014-05-03 12:12:12:1234', 'many urls', 'checksum', 'meta_url', 'input 3', 'abhinav', 'stanford');

insert into OutgoingData(project, name, version, timestamp, urls, checksums, metadata_url, comments, username, hostname) values (2, 'init', '0.01', '2014-05-03 12:12:12:1234', 'many urls', 'checksum', 'meta_url', 'output 1', 'abhinav', 'stanford');
insert into OutgoingData(project, name, version, timestamp, urls, checksums, metadata_url, comments, username, hostname) values (2, 'two', '0.01', '2014-05-03 12:12:12:1234', 'many urls', 'checksum', 'meta_url', 'output 2', 'abhinav', 'stanford');
insert into OutgoingData(project, name, version, timestamp, urls, checksums, metadata_url, comments, username, hostname) values (1, 'three', '0.01', '2014-05-03 12:12:12:1234', 'many urls', 'checksum', 'meta_url', 'output 3', 'abhinav', 'stanford');
