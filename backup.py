#!/usr/bin/env python2
# -*- coding: utf-8 -*-

# backup.py, version 0.0.12 [2012-07-03]
# Copyright (C) 2011-2012 Martin Wegner.
# Released under the terms of "THE BEER-WARE LICENSE" (Revision 42):
# <mw@mroot.net> wrote this file. As long as you retain this notice you
# can do whatever you want with this stuff. If we meet some day, and you think
# this stuff is worth it, you can buy me a beer in return.

# TODO: Refresh today's backup

import os         # path and filesystem reading/manipulation
import re         # Expressions. Regular Style!
import shlex      # Used in shell wrappers to (re-)parse command
import subprocess # Used to execute shell commands
import sys        # argv
import time       # I'd like to know when today is...

# Color definitions:
if sys.stdout.isatty():
	colors = { 'blue':   '[1;34m'
	         , 'yellow': '[1;33m'
	         , 'white':  '[1;37m'
	         , 'reset':  '[00m'
	         }
else:
	colors = { 'blue': '', 'yellow': '', 'white': '', 'reset': '' }

def showUsage():
	print '', colors[ 'blue' ] + '*' + colors[ 'white' ], os.path.basename( sys.argv[ 0 ] ), 'version 0.0.12 [2012-07-03]'
	print '', colors[ 'blue' ] + '*' + colors[ 'white' ], 'Released unter the terms of "THE BEER-WARE LICENSE", see source for full license text ;-)'
	print colors[ 'reset' ]
	targets = [ 'local', 'remote' ]
	operations = [ 'mount', 'backup', 'umount' ]
	for t in targets:
		for op in operations:
			print '\t', sys.argv[ 0 ], t, op,
			print '\t\t', getattr( getattr( sys.modules[ __name__ ], 'Backup' + t.title() ), op ).__doc__

###
### Exceptions:
###

class ExceptionMountReadOnly( RuntimeWarning ):
	def __init__( self, device ):
		self.description = 'Device ' + device + ' is mounted read/only...'

	def __str__( self ):
		return repr( self.description )

class ExceptionParseError( RuntimeError ):
	def __init__( self, description ):
		self.description = 'Parse error: ' + description

	def __str__( self ):
		return repr( self.description )

class ExceptionNoSpaceLeft( RuntimeError ):
	def __init__( self, device ):
		self.description = 'No space left on ' + device

	def __str__( self ):
		return repr( self.description )

###
### Helpers:
###

paths = { 'sudo':       '/usr/bin/sudo'
        , 'cryptsetup': '/sbin/cryptsetup'
        , 'mount':      '/bin/mount'
        , 'umount':     '/bin/umount'
        , 'df':         '/bin/df'
        , 'du':         '/usr/bin/du'
        , 'rm':         '/bin/rm'
        , 'mkdir':      '/bin/mkdir'
        , 'chown':      '/bin/chown'
        , 'rsync':      '/usr/bin/rsync'
        , 'encfs':      '/usr/bin/encfs'
        , 'fusermount': '/usr/bin/fusermount'
        }

def printStatus( *args, **kwargs ):
	print kwargs[ 'indent' ] if kwargs.has_key( 'indent' ) else '', colors[ 'yellow' ] + '*' + colors[ 'white' ] + ' ' + ' '.join( map( str, list( args ) ) ) + colors[ 'reset' ]

def call( command, *args ):
	print colors[ 'blue' ] + '->' + colors[ 'white' ], command, ' '.join( args ), colors[ 'reset' ]
	command = shlex.split( command + ' ' + ' '.join( args ) )
	subprocess.call( command )

def check_output( command, *args ):
	#print colors[ 'blue' ] + '->' + colors[ 'white' ], command, ' '.join( args ), colors[ 'reset' ]
	command = shlex.split( command + ' ' + ' '.join( args ) )
	return subprocess.check_output( command )

class LocalCryptDevice:
	_mapperPrefix = '/dev/mapper/'
	rawDevice = ''
	mapperTarget = ''
	mountDestination = ''

	def __init__( self, rawDevice, mapperTarget, mountDestination ):
		self.rawDevice = rawDevice
		self.mapperTarget = mapperTarget
		self.mountDestination = mountDestination
		self._mapperDevice = os.path.normpath( self._mapperPrefix + os.sep + self.mapperTarget )

	def _isMounted( self, device, destination):
		mountOutput = check_output( paths[ 'mount' ] )
		matchMount = re.search( '^' + re.escape( device ) + ' on ' + re.escape( destination ) + ' type \S+ \((\S+)\)$', mountOutput, re.IGNORECASE | re.MULTILINE )
		if matchMount is not None:
			mountOptions = matchMount.group( 1 )
			if mountOptions.find( 'ro' ) != -1:
				raise ExceptionMountReadOnly( device )
			else:
				return True
		else:
			return False

	def _checkFreeSpace( self, device ):
		dfOutput = check_output( paths[ 'df' ], device )
		matchFreeSpace = re.search( '^' + re.escape( device ) + '\s+[0-9]+\s+[0-9]+\s+([0-9]+)', dfOutput, re.IGNORECASE | re.MULTILINE )
		if matchFreeSpace is not None:
			return int( matchFreeSpace.group( 1 ) )
		else:
			raise ExceptionParseError( 'Failed to parse output of ' + paths[ 'df' ] + ' for ' + device + '.' )

	def mount( self ):
		call( paths[ 'sudo' ]
		    , paths[ 'cryptsetup' ]
		    , 'luksOpen'
		    , self.rawDevice
		    , self.mapperTarget
		    )
		try:
			if not self._isMounted( self._mapperDevice, self.mountDestination ):
				call( paths[ 'sudo' ]
				    , paths[ 'mount' ]
				    , self._mapperDevice
				    , self.mountDestination
				    )
			else:
				printStatus( self._mapperDevice, 'is already mounted on', self.mountDestination )
		except ExceptionMountReadOnly as e:
			print e
			printStatus( 'Remounting read/write...' )
			call( paths[ 'sudo' ], paths[ 'mount' ], '-o remount,rw', self._mapperDevice )

	def umount( self ):
		call( paths[ 'sudo' ], paths[ 'umount' ], self.mountDestination )
		call( paths[ 'sudo' ], paths[ 'cryptsetup' ], 'luksClose', self.mapperTarget )

###
### Backup classes
###

class BackupRemote:
	_localDevice = None
	config = { 'rawDevice':         '/dev/disk/by-uuid/XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX'
	         , 'mapperTarget':      'crypt-backup'
	         , 'mountDestination':  '/mnt/backup'
	         , 'encfsSource':       './.encfs-hidrive-backup/'
	         , 'encfsTarget':       './hidrive-backup/'
	         , 'backupTargets':     [ '/home/XXXXXX/Documents'
	                                , '/home/XXXXXX/Photos'
	                                , '/home/XXXXXX/Projects'
	                                ]
	         , 'backupExcludes':    []
	         , 'remoteUser':        'XXXXXXX'
	         , 'remoteHost':        'sftp.hidrive.strato.com'
	         , 'remotePort':        22
	         , 'remoteDestination': '/users/XXXXXXX/backup/'
	         }

	def __init__( self ):
		self._encfsSource = os.path.normpath( self.config[ 'mountDestination' ] + os.sep + self.config[ 'encfsSource' ] )
		self._encfsTarget = os.path.normpath( self.config[ 'mountDestination' ] + os.sep + self.config[ 'encfsTarget' ] )
		self._localDevice = LocalCryptDevice( self.config[ 'rawDevice' ]
		                                    , self.config[ 'mapperTarget' ]
		                                    , self.config[ 'mountDestination' ]
		                                    )

	def mount( self ):
		"""
		Mounts a local backup device containing a source for an encfs
		and opens the encfs source.
		"""
		self._localDevice.mount()
		call( paths[ 'encfs' ], self._encfsSource, self._encfsTarget )

	def backup( self ):
		"""
		Backups selected local source directories into an encfs on a
		local backup device and transfers the (encrypted) encfs source
		to a configured remote location.
		"""
		call( paths[ 'rsync' ], '--delete-before', '--delete-excluded', '-a', '--progress'
		    , ' '.join( [ '--exclude=' + e for e in self.config[ 'backupExcludes' ] ] )
		    , ' '.join( self.config[ 'backupTargets' ] )
		    , self._encfsTarget + os.sep # trailing slash is important for rsync
		    )
		call( paths[ 'rsync' ], '--delete-before', '-a', '--progress'
		    , '-e "ssh -p ' + str( self.config[ 'remotePort' ] ) + '"'
		    , self._encfsSource
		    , self.config[ 'remoteUser' ] + '@' + self.config[ 'remoteHost' ] + ':' + self.config[ 'remoteDestination' ]
		    )

	def umount( self ):
		"""
		Unmounts the encfs used for the remote backup and also the
		underlying local backup device.
		"""
		call( paths[ 'fusermount' ], '-u', self._encfsTarget )
		self._localDevice.umount()

class BackupLocal:
	_mapperPrefix = '/dev/mapper/'
	_localDevice = None
	config = { 'keep':              2 # number of backups to keep
	         , 'rawDevice':         '/dev/disk/by-uuid/XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX'
	         , 'mapperTarget':      'crypt-backup'
	         , 'mountDestination':  '/mnt/backup'
	         , 'backupDestination': './XXXXX/'
	         , 'backupTargets':     [ '/'
	                                , '/home'
	                                , '/var'
	                                ]
	         , 'backupExcludes':    [ '/dev'
	                                , '/lost+found'
	                                , '/proc'
	                                , '/run'
	                                , '/sys'
	                                , '/tmp'
	                                , '.gvfs'
	                                ]
	         }

	def __init__( self ):
		self._debug = False
		self._mapperDevice = os.path.normpath( self._mapperPrefix + os.sep + self.config[ 'mapperTarget' ] )
		self._backupLocation = os.path.normpath( self.config[ 'mountDestination' ] + os.sep + self.config[ 'backupDestination' ] )
		self._localDevice = LocalCryptDevice( self.config[ 'rawDevice' ]
		                                    , self.config[ 'mapperTarget' ]
		                                    , self.config[ 'mountDestination' ]
		                                    )

	def mount( self ):
		"""
		Mounts a local device via dm-crypt as backup destination.
		"""
		self._localDevice.mount()
		if '/boot' in self.config[ 'backupTargets' ]:
			call( paths[ 'sudo' ], paths[ 'mount' ], '/boot' )

	def backup( self ):
		"""
		Incrementally backups whole system (as configured) to the mounted local backup device.
		"""
		backups = self._listBackups( self._backupLocation )
		if self._debug:
			print backups
		# * no backups found, thus full backup:
		if len( backups ) == 0:
			printStatus( 'Doing first full backup...' )
			linkOptions = ''
		# * one (full) backup found, so linking to it:
		elif len( backups ) == 1:
			printStatus( 'Full backup found, doing first incremental backup...' )
			linkOptions = '--link-dest=' + '..' + os.sep + backups[ -1 ]
		# * more than one backup found, thus incremental backup linking to newest old backup
		# *   with possible wiping of old backups if not enough space:
		else:
			printStatus( 'Incremental backup from ' + backups[ -1 ] )
			print 'Using heuristic to determine incremental size...'
			# Determine size of last two backups, due to hardlinks 'du' will print
			# only changed files' size for the latest backup, giving the size of the incremental backup:
			freeSpace = self._localDevice._checkFreeSpace( self._mapperDevice )
			duOutput = check_output( paths[ 'sudo' ], paths[ 'du' ]
			                       , '--max-depth=1'
			                       , os.path.normpath( self._backupLocation + os.sep + backups[ -2 ] )
			                       , os.path.normpath( self._backupLocation + os.sep + backups[ -1 ] )
			                       )
			try:
				lastBackupIncrementalSize = int( re.search( '^([0-9]+)\s', str.splitlines( duOutput )[ -1 ] ).group( 1 ) )
			except ( AttributeError, IndexError ):
				raise ExceptionParseError( 'Failed to parse output of ' + paths[ 'du' ] + ' when determining size of the two latest backups.' )
			if self._debug:
				print lastBackupIncrementalSize, '>', freeSpace, '?'
			# If there is not sufficient space, clean oldest backups:
			if lastBackupIncrementalSize > freeSpace:
				backups = self._cleanOldBackups( lastBackupIncrementalSize )
			# Also, link to latest backup:
			linkOptions = '--link-dest=' + '..' + os.sep + backups[ -1 ]
		# By now, there should be enough free space...
		#   ...or we should have aborted already with ExceptionNoSpaceLeft
		#   ...or we could not determine the size of the new backup...
		# So, create new backup folder:
		currentBackupLocation = os.path.normpath( self._backupLocation + os.sep + time.strftime( '%Y-%m-%d' ) ) + os.sep # trailing slash is needed for rsync
		call( paths[ 'sudo' ], paths[ 'mkdir' ], '-p', currentBackupLocation )
		call( paths[ 'sudo' ], paths[ 'chown' ], '700', currentBackupLocation )
		# Finally call rsync, let's do the backup ... uh-uh, let's do it ...
		call( paths[ 'sudo' ], paths[ 'rsync' ]
		    , ' '.join( [ '--exclude=' + e for e in self.config[ 'backupExcludes' ] ] )
		    , '--delete-before'
		    , '--delete-excluded'
		    , '--progress'
		    , '-aRx'
		    , linkOptions
		    , ' '.join( self.config[ 'backupTargets' ] )
		    , currentBackupLocation
		    )

	def umount( self ):
		"""
		Umounts the local backup device.
		"""
		if '/boot' in self.config[ 'backupTargets' ]:
			call( paths[ 'sudo' ], paths[ 'umount' ], '/boot' )
		self._localDevice.umount()

	def _listBackups( self, destination ):
		return sorted( filter( lambda d: re.search( '^[0-9]{4}-[0-9]{2}-[0-9]{2}$', d ) is not None, os.listdir( destination ) ) )

	def _cleanOldBackups( self, requiredSize ):
		freeSpace = self._localDevice._checkFreeSpace( self._mapperDevice )
		backups = self._listBackups( self._backupLocation )
		printStatus( 'Not enough space, cleaning old backups...' )
		while requiredSize > freeSpace and len( backups ) > self.config[ 'keep' ]:
			printStatus( 'Wiping backup', backups[ 0 ], indent = '\t' )
			oldBackupPath = os.path.normpath( self._backupLocation + os.sep + backups[ 0 ] )
			if self._debug:
				call( paths[ 'sudo' ], 'echo', paths[ 'rm' ], '-Rf', oldBackupPath )
			else:
				call( paths[ 'sudo' ], paths[ 'rm' ], '-Rf', oldBackupPath )
			# Update variables to current state:
			backups = self._listBackups( self._backupLocation )
			freeSpace = self._localDevice._checkFreeSpace( self._mapperDevice )
		# Check if we cleaned all possible backups while keeping config[ 'keep' ] backups,
		# if there is still not enough space, abort with ExceptionNoSpaceLeft:
		if len( backups ) == self.config[ 'keep' ] and requiredSize > freeSpace:
			printStatus( 'Won\'t clean last ' + self.config[ 'keep' ] + ' backup(s)...' )
			printStatus( 'Not enough space left after cleaning up, please take care of it yourself...' )
			raise ExceptionNoSpaceLeft( self._mapperDevice )
		else:
			return backups

if __name__ == '__main__':
	try:
		target = sys.argv[ 1 ]
		operation = sys.argv[ 2 ]
		if operation[ 0 ] == '_':
			raise AttributeError
		# Instantiate Backup{Local,Remote} class as requested:
		backup = getattr( sys.modules[ __name__ ], 'Backup' + target.title() )()
		# Call requested method:
		getattr( backup, operation )()
	except ( IndexError, AttributeError ):
		showUsage()

