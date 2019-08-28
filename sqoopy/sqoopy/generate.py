#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Usage: generate.py <host> <database> [--port=port]
[--tables=tables] [--sqoop_options=sqoop_options] [--oozie] 

sqoopy: Generate sqoop custom import statements

Arguments:
	host		the host name of the MySQL database
	database	name of the database
	port        the port of the MySQL database, default is 3306
	tables		comma separated list of tables that need to be inspected
	sqoop_options	Append verbatim sqoop command line options

'''

"""
sqoopy: Generate sqoop custom import statements

Copyright (C) 2012  Diederik van Liere, Wikimedia Foundation

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
"""

import subprocess
import re
import sys
import logging
import math
from docopt import docopt
from collections import OrderedDict



log = logging.getLogger()
log.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
log.addHandler(ch)
re1=r'\(\d{1,5}\)'
re2= r'\(Max\)'
column_size = re.compile("(%s|%s)" % (re1, re2))

class Column(object):
	def __init__(self, name, datatype, size, pk):
		self.name = name
		self.datatype = datatype
		self.size = size
		self.pk = pk
	
	def __str__(self):
		return '%s (%s)' % (self.name, self.datatype)

class Datatype(object):
	def __init__(self):
		self.hive_types = set(['Bigint', 'Boolean', 'Float', 'Double', 'String', 'Binary']) 
		'''
		Mysql to Mysql Casting
		'''
		self.mysql_to_mysql = {}
		self.mysql_to_mysql['Varbinary'] = 'Char'
		self.mysql_to_mysql['Binary'] = 'Char'
		self.mysql_to_mysql['Blob'] = 'Char'
		self.mysql_to_mysql['Tinyblob'] = 'Char'
		self.mysql_to_mysql['Mediumblob'] = 'Char'
		self.mysql_to_mysql['Tinyint'] = 'Integer'
		self.mysql_to_mysql['Datetime2'] = 'Datetime'
		self.mysql_to_mysql['Smalldatetime'] = 'Datetime'
		self.mysql_to_mysql['Numeric'] = 'Integer'
		self.mysql_to_mysql['Bit'] = 'Int'
		'''
		Mysql to Mysql replace
		'''
		self.mysql_to_mysql_replace = {}
		self.mysql_to_mysql_replace['Nvarchar'] = 'Nvarchar'
		self.mysql_to_mysql_replace['Nchar'] = 'Nchar'
		self.mysql_to_mysql_replace['Varchar'] = 'Varchar'
		self.mysql_to_mysql_replace['Char'] = 'Char'
		self.mysql_to_mysql_replace['Text'] = 'Text'

		'''
		Mysql to Hive Casting
		'''
		self.mysql_to_hive = {}
		self.mysql_to_hive['Image'] = 'String'
		self.mysql_to_hive['Xml'] = 'String'
		self.mysql_to_hive['Text'] = 'String'
		self.mysql_to_hive['Uniqueidentifier'] = 'String'
		self.mysql_to_hive['Nchar'] = 'Char'
		self.mysql_to_hive['Ntext'] = 'String'
		self.mysql_to_hive['Nvarchar'] = 'String'
		self.mysql_to_hive['Real'] = 'Float'
		self.mysql_to_hive['Varbinary'] = 'Binary'
		self.mysql_to_hive['Binary'] = 'Binary'
		self.mysql_to_hive['Blob'] = 'Binary'
		self.mysql_to_hive['Tinyblob'] = 'Binary'
		self.mysql_to_hive['Mediumblob'] = 'Binary'
		self.mysql_to_hive['Timestamp'] = 'String'
		self.mysql_to_hive['Varchar'] = 'Varchar'
		self.mysql_to_hive['Char'] = 'Char'
		self.mysql_to_hive['Enum'] = 'Int'
		self.mysql_to_hive['Int'] = 'Int'
		self.mysql_to_hive['Datetime'] = 'TIMESTAMP'
		self.mysql_to_hive['Smallint'] = 'Int'
		self.mysql_to_hive['Decimal'] = 'Decimal'
		self.mysql_to_hive['Integer'] = 'Int'
		self.size = {}
		self.size['timestamp'] = 19
		
	def supports(self, mysql_datatype):
		return True if mysql_datatype in self.hive_types else False

	def requires_mysql_cast(self, mysql_datatype):
		if mysql_datatype in self.mysql_to_mysql.keys():
			return True
		else:
			return False

	def requires_mysql_replace(self, mysql_datatype):
		if mysql_datatype in self.mysql_to_mysql_replace.keys():
			return True
		else:
			return False

	def convert(self, mysql_datatype, destination='hive'):
		if destination == 'hive':
			if self.supports(mysql_datatype):
				return mysql_datatype
			else:
				if self.requires_mysql_cast(mysql_datatype):
					mysql_datatype = self.mysql_to_mysql.get(mysql_datatype)
				return self.mysql_to_hive.get(mysql_datatype, '%s has not yet a hive mapping ' % mysql_datatype)
		elif destination == 'mysql':
			return self.mysql_to_mysql.get(mysql_datatype)
		else:
			raise Exception('Destination %s is not supported' % destination)
			sys.exit(-1)

class Db(object):
	def __init__(self, host, database, port=3306, tables=None, sqoop_options=None):
		self.host = host
		self.port = port
		self.database = database
		self.tables = tables if tables else [] 
		self.sqoop_options = sqoop_options if sqoop_options != None else ''
		self.data = None
		self.row_count = 0
		self.blocksize = (1024 ** 3) * 256  # Hardcoded default for now
		self.schema = OrderedDict()
		self.verbose = True
		self.mysql_cmd = ['SQLCMD.EXE', '-S%s' % self.host, '-E','-d%s'% self.database, '-h-1', '-s&']
		self.sqoop_cmd = 'sqoop import -libjars libs/jtds-1.3.1.jar  -Dorg.apache.sqoop.splitter.allow_text_splitter=true   --connect \'jdbc:jtds:sqlserver://%s:%s;databaseName=%s;useNTLMv2=true;domain=EXPLOITATION;integratedSecurity=true \' --driver net.sourceforge.jtds.jdbc.Driver   --hive-database %s  %s' % (self.host, self.port,self.database, '_'.join([self.database,"temp"]), self.sqoop_options)#pour le moment l'import via parquet implique le passage par une bd temp

#juste après hive data base --target-dir data/hive/test/DPLAN_INTERVENTION_PLANNING
	def __str__(self):
		return '%s:%s' % (self.host, self.database)

	def get_pk(self, table):
		for name, column in self.schema.iteritems():
			if column.pk is True:
				return name
		#return self.schema.values()[0].name #if no primary key take first col for sqoop split
		return False

	def launch(self, query):	
		p = subprocess.Popen(self.mysql_cmd, shell=False, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
		stdoutdata, stderrdata = p.communicate(query)
		if stderrdata:
			raise Exception('The following error was encountered:\n %s' % stderrdata)
			#log.error('Encountered error: %s' % stderrdata)
			sys.exit(-1)
		stdoutdata = stdoutdata.split('\r')
		return stdoutdata[:-1]

	def get_row_count(self, table):
		query = "SELECT * FROM TABLE_ROWS () WHERE bob ='%s' AND TABLEName ='%s';" % (self.database, table)

	def get_tables(self):
		self.tables = []
		tables = self.launch('SELECT TABLE_NAME FROM  INFORMATION_SCHEMA.TABLES;')
		for table in tables:
			#if self.verbose:
				#log.info('Found table: %s' % table)
			self.tables.append(table)

	def inspect(self, table):
		self.data = self.launch("set nocount on;select * from dbo.describe ('%s')" % table)

	def create_schema(self, table):
		#self.schema ={}
		self.schema =OrderedDict()
		mapping = Datatype()
		for data in self.data:
			data = data.split('&')
			name = re.sub(r'\s+', '', data[0])
			datatype = re.split(column_size, re.sub(r'\s+', '', data[2]))[0]
			datatype = datatype.lower().capitalize() 
			datatype = re.sub(r'\(\d{0,5},\d{0,5}\)|\(\)','',datatype)
			size = re.findall(column_size, re.sub(r'\s+', '', data[2]))
			if len(size) > 0:
				size = int(re.sub(r'Max', '65535',size[0][1:-1]))
			else:
				size = mapping.size.get(datatype, 0)
			pk = True if re.sub(r'\s+', '', data[1]) == 'YES' else False
			column = Column(name, datatype, size, pk)
			self.schema.setdefault(name, column)


	def cast_columns(self):
		query = ''
		converter = Datatype()
		for name, column in self.schema.iteritems():
			if converter.requires_mysql_cast(column.datatype):
				charset = '' #'CHARACTER SET utf8' if column.datatype.find('binary') == -1 else ''
				part = 'CAST(%s AS %s %s)' % (name, converter.mysql_to_mysql.get(column.datatype), charset)
			else:
				part = name
			if converter.requires_mysql_replace(column.datatype):
				if column.datatype == 'Text':
					part = 'cast(replace(REPLACE(cast(%s as NVarchar(MAX)) ,CHAR(13), \\\' \\\' ),CHAR(10),\\\' \\\')as Text) as %s' %(part,name)
				else:
					part = 'replace(REPLACE(%s ,CHAR(13), \\\' \\\' ),CHAR(10),\\\' \\\') as %s' %(part,name)
			else:
				part = '%s as %s'%(part,name)
			query = ', '.join([query, part])
		return query[1:]
	
	def number_of_mappers(self, table):
		self.get_row_count(table)
		row_size = sum([column.size for column in self.schema.itervalues()]) + len(self.schema.keys())
		num_mappers = int(math.ceil((self.row_count * row_size) / self.blocksize))
		if num_mappers < 5:
			return 4
		else:
			return num_mappers

	def generate_query(self, query_type, query, table):
		'''
		About importance of $CONDITIONS, see:
		https://groups.google.com/a/cloudera.org/forum/?fromgroups#!topic/sqoop-user/Z9Wa2ISpRvI
		
		Valid Sqoop import statement using custom SQL select query
		sqoop import --username <username> -P --target-dir /foo/bar 
			--connect jdbc:mysql://localhost:3306/db_name 
			--split-by rc_id 
			--query 'SELECT rc_id,CAST(column AS char(255) CHARACTER SET utf8) AS column FROM table_name WHERE $CONDITIONS'
		'''
		if query_type == 'select':			
			query = 'SELECT %s FROM %s WHERE \\\\$CONDITIONS' % (query, table)
			#if self.verbose:
				#log.info('Constructed query: %s' % query)
		else:
			raise Exception('Query type %s not yet supported' % query_type)
		return query

	def mapping_hive(self, table):
		converter=Datatype()
		mapping = ''
		for name, column in self.schema.iteritems():
			col_map = converter.convert(column.datatype)
			if (col_map=="Char") | (col_map=="Varchar"):
				if not column.size:
					column.size=1
				if (col_map=="Char") & (column.size>255):
					column.size=255
				part_map = '%s=\'%s(%s)\'' % (column.name, col_map,column.size)
			else :
				part_map = '%s=%s' % (column.name, col_map)
			mapping = ','.join([mapping, part_map])
		return mapping[1:]
	




	def generate_sqoop_cmd(self, mappers, query, table,mapping_hive):
		pk = self.get_pk(table)
		if pk == False :
			split_by = ''
			mappers = '-m 1'
		else :
			split_by = '--split-by %s' % pk
			mappers = '--num-mappers %s' % mappers
		query = '--query "%s"' % query
		target_dir = '--target-dir data/temp/%s/%s' % (self.database, table)
		#hive_commands = '--create-hive-table --hive-table %s_%s --hive-import --hive-overwrite' % (self.database, table)
		hive_commands = '--create-hive-table --hive-table %s --hive-import --hive-overwrite --delete-target-dir --null-string \'\\\\\\\\N\' --null-non-string \'\\\\\\\\N\'' % (table) #retrait du nom de la bdd dans le nom de table
		mapping_hive = '--hive-drop-import-delims --map-column-hive %s' % mapping_hive
		sqoop_cmd = ' '.join([self.sqoop_cmd, hive_commands, split_by, mappers, target_dir, query, mapping_hive])
		
		#if self.verbose:
			#log.info('Generated sqoop command: %s' % sqoop_cmd)
		return sqoop_cmd

def run(args):
	'''
	Given a mysql database name and an optional table, construct a select query 
	that takes care of casting (var)binary and blob fields to char fields.
	'''
	sqoop_options = args.get('--sqoop_options') if args.get('--sqoop_options') != None else ''
	database = Db(args.get('<host>'),args.get('<database>'), args.get('--port'), args.get('--tables'), args.get('--target_dir'))
	if not args.get('--tables'):
		database.get_tables()
	else:
		database.tables = args.get('--tables').split(',')
	fh = open('sqoop.sh', 'w')
	#log.info('Opening file handle...')
	for table in database.tables:
		database.inspect(table)
		database.create_schema(table)
		query = database.cast_columns()
		query = database.generate_query('select', query, table)
		mappers = database.number_of_mappers(table)
		mapping_hive = database.mapping_hive(table)
		sqoop_cmd = database.generate_sqoop_cmd(mappers, query, table, mapping_hive)
		fh.write(sqoop_cmd)
		fh.write('\n')
	fh.close()
	#log.info('Closing filehandle.')
	#log.info('Exit successfully, close Sqoopy.')

def main():
	'Main script entrypoint for CLI.'
	#log.info('Initializing command line parameters.')
	args = docopt(__doc__)
	run(args)


if __name__ == '__main__':
	#log.info('Starting sqoopy')
	main()
