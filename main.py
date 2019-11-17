import xml.etree.ElementTree as ET
import tableauserverclient as TSC
import copy
import psycopg2
import random
import string
import re
import os
import argparse
import configparser
from xml.dom import minidom
from psycopg2 import sql
from models.table_metadata import TableMetadata
import json

# Datatypes that are assigned the role 'measure'.
measureTypes = ['real', 'spatial', 'integer']
# Datatypes that are assigned the type 'ordinal'.
ordinalTypes = ['datetime', 'date']
# Datatypes that are assigned the type 'quantitative'.
quantitativeTypes = ['real', 'integer']

config = configparser.ConfigParser()
config.read('config.ini')

tableauServerAddress = config['TABLEAU_SERVER']['TableauServerAddress']
verbose = config['MISC'].getboolean('Verbose')
outputDirectory = config['MISC']['OutputDirectory'] if config['MISC']['OutputDirectory'] else os.getcwd()

dataSourceProjectId = config['TABLEAU_SERVER']['ProjectId']
dataSourceName = config['TABLEAU_SERVER']['DatasourceName']
pgHost = config['POSTGRESQL']['Host']
pgPort = config['POSTGRESQL']['Port']
pgUser = config['POSTGRESQL']['User']
pgPassword = config['POSTGRESQL']['Password']
pgSchemaName = config['POSTGRESQL']['Schema']
dbName = config['POSTGRESQL']['Database']
dbTables = config['POSTGRESQL']['Tables'].split(',')
useCustomSQL = config['POSTGRESQL'].getboolean('UseCustomSql')
customSQL = config['POSTGRESQL']['CustomSql']
pgSslMode = 'require' if config['POSTGRESQL'].getboolean(
    'RequireSsl') else 'allow'
tableauIgnoreSslCert = config['TABLEAU_SERVER'].getboolean(
    'IgnoreSslVerification')
tableauUserId = config['TABLEAU_SERVER']['User']
tableauPassword = config['TABLEAU_SERVER']['Password']


def generateTdsFiles():
    conn = None
    try:
        regtype_mappings = None

        with open(os.path.join('mappings', 'postgresql.json'), 'r') as fp:
            regtype_mappings = json.load(fp)

        if verbose:
            print('Initiating connection to DB: {}'.format(dbName))
            lineClear()

        conn = psycopg2.connect(
            dbname=dbName, user=pgUser, password=pgPassword, host=pgHost, port=pgPort)
        cur = conn.cursor()

        # Only generate TDS file for the provided custom SQL.
        # A temporary table is created in order to get column data type, which is necessary for TDS file generation.
        if useCustomSQL:
            if verbose:
                print(
                    'Using custom SQL')

            tempTableName = 'temp_{}'.format(
                ''.join(random.choices(string.ascii_lowercase + string.digits, k=15)))
            cur.execute(
                sql.SQL('CREATE TEMP TABLE {} AS {}'.format(tempTableName, customSQL)))
            conn.commit()
            cur.execute(sql.SQL('SELECT table_name FROM information_schema.tables WHERE table_name={}').format(
                sql.Literal(tempTableName)))
        # Generate TDS files for all specified tables
        elif len(dbTables) > 0:
            cur.execute(sql.SQL('SELECT table_name FROM information_schema.tables WHERE table_schema={} and table_name IN ({})').format(
                sql.Literal(pgSchemaName), sql.SQL(', ').join(sql.Literal(n) for n in dbTables)))
        # Generate TDS files for all tables in the specified schema
        else:
            cur.execute(sql.SQL('SELECT table_name FROM information_schema.tables WHERE table_schema={}').format(
                sql.Literal(pgSchemaName)))

        tableNames = cur.fetchall()

        if verbose:
            print('Found {} {} tables'.format(str(len(tableNames)),
                                              ' temp' if not useCustomSQL else ''))
            lineClear()

        for tableName in tableNames:
            if verbose:
                print('Processing table: {}'.format(tableName[0]))

            tableMetadata = copy.deepcopy(
                TableMetadata(tableName=tableName[0]))

            # Temporary tables are in a dynamically named schema
            if useCustomSQL:
                cur.execute(sql.SQL('SELECT column_name, udt_name::regtype, ordinal_position FROM information_schema.columns WHERE table_name={}').format(
                    sql.Literal(tableMetadata.TableName)))
            else:
                cur.execute(sql.SQL('SELECT column_name, udt_name::regtype, ordinal_position FROM information_schema.columns WHERE table_schema={} AND table_name={}').format(
                    sql.Literal(pgSchemaName), sql.Literal(tableMetadata.TableName)))
            columns = cur.fetchall()

            if verbose:
                print('Found {} columns'.format(str(len(columns))))

            for column in columns:
                columnMetadata = regtype_mappings[column[1]]

                columnMetadata['LocalName'] = column[0]
                columnMetadata['Ordinal'] = column[2]
                columnMetadata['ParentName'] = tableMetadata.TableName if not useCustomSQL else 'Custom SQL Query'
                columnMetadata['RemoteAlias'] = column[0]
                columnMetadata['RemoteName'] = column[0]

                tableMetadata.ColumnMetadata.append(columnMetadata)

            filePath = os.path.join(
                outputDirectory, (tableMetadata.TableName if not useCustomSQL else dataSourceName) + '.tds')

            writeXML(tableMetadata, filePath)

            if verbose:
                print('Finished processing table: {}'.format(tableName[0]))
                print('TDS generation complete. File is located at {}'.format(
                    os.path.abspath(filePath)))
                lineClear()

            publishDataSource(filePath, dataSourceProjectId)

            cur.close()
    except psycopg2.Error as e:
        print(e)
    finally:
        if verbose:
            print('Closing connection to DB: {}'.format(dbName))

        if conn is not None:
            conn.close()


def lineClear():
    print('----------')


def writeXML(tableMetadata, filePath):
    dataSource = ET.Element('datasource')

    if useCustomSQL:
        dataSource.set('formatted-name',
                       'custom.{}'.format(''.join(random.choices(string.ascii_lowercase + string.digits, k=28))))
    else:
        dataSource.set('formatted-name',
                       'federated.{}'.format(''.join(random.choices(string.ascii_lowercase + string.digits, k=28))))
    dataSource.set('inline', 'true')
    dataSource.set('source-platform', 'win')
    dataSource.set('version', '18.1')
    dataSource.set('xmlns:user', 'http://www.tableausoftware.com/xml/user')

    connection = ET.SubElement(dataSource, 'connection')
    connection.set('class', 'federated')

    namedConnections = ET.SubElement(connection, 'named-connections')

    namedConnection = ET.SubElement(namedConnections, 'named-connection')
    namedConnection.set('caption', pgHost)
    namedConnection.set('name', 'postgres.{}'.format(
        ''.join(random.choices(string.ascii_lowercase + string.digits, k=28))))

    connectionInner = ET.SubElement(namedConnection, 'connection')
    connectionInner.set('authentication', 'username-password')
    connectionInner.set('class', 'postgres')
    connectionInner.set('dbname', dbName)
    connectionInner.set('odbc-native-protocol', '')
    connectionInner.set('one-time-sql', '')
    connectionInner.set('port', pgPort)
    connectionInner.set('server', pgHost)
    connectionInner.set('sslmode', pgSslMode)
    connectionInner.set('username', pgUser)

    relation = ET.SubElement(connection, 'relation')
    relation.set('connection', namedConnection.get('name'))
    relation.set(
        'name', tableMetadata.TableName if not useCustomSQL else 'Custom SQL Query')

    if not useCustomSQL:
        relation.set('table', '[{}].[{}]'.format(
            pgSchemaName, tableMetadata.TableName))

    relation.set('type', 'table' if not useCustomSQL is None else 'text')

    if useCustomSQL:
        relation.text = customSQL

    metadataRecords = ET.SubElement(connection, 'metadata-records')

    for record in tableMetadata.ColumnMetadata:
        metadataRecord = ET.SubElement(metadataRecords, 'metadata-record')
        metadataRecord.set('class', 'column')

        remoteName = ET.SubElement(metadataRecord, 'remote-name')
        remoteName.text = record['RemoteName']

        remoteType = ET.SubElement(metadataRecord, 'remote-type')
        remoteType.text = str(record['RemoteType'])

        localName = ET.SubElement(metadataRecord, 'local-name')
        localName.text = '[{}]'.format(record['LocalName'])

        parentName = ET.SubElement(metadataRecord, 'parent-name')
        parentName.text = '[{}]'.format(record['ParentName'])

        remoteAlias = ET.SubElement(metadataRecord, 'remote-alias')
        remoteAlias.text = record['RemoteAlias']

        ordinal = ET.SubElement(metadataRecord, 'ordinal')
        ordinal.text = str(record['Ordinal'])

        localType = ET.SubElement(metadataRecord, 'local-type')
        localType.text = record['LocalType']

        containsNull = ET.SubElement(metadataRecord, 'contains-null')
        containsNull.text = str(record['ContainsNull']).lower()

        if record['Aggregation'] is not None:
            aggregation = ET.SubElement(metadataRecord, 'aggregation')
            aggregation.text = record['Aggregation']

        if record['Width'] is not None:
            width = ET.SubElement(metadataRecord, 'width')
            width.text = str(record['Width'])

        if record['Precision'] is not None:
            precision = ET.SubElement(metadataRecord, 'precision')
            precision.text = str(record['Precision'])

        if record['Scale'] is not None:
            scale = ET.SubElement(metadataRecord, 'scale')
            scale.text = str(record['Scale'])

        if record['Collation']:
            collation = ET.SubElement(metadataRecord, 'collation')
            collation.set('flag', '0')
            collation.set('name', 'LEN_RUS')

        if record['PaddedSemantics']:
            paddedSemantics = ET.SubElement(metadataRecord, 'padded-semantics')
            paddedSemantics.text = str(record['PaddedSemantics']).lower()

        if record['CastToLocalType']:
            castToLocalType = ET.SubElement(
                metadataRecord, 'cast-to-local-type')
            castToLocalType.text = str(record['CastToLocalType']).lower()

        attributes = ET.SubElement(metadataRecord, 'attributes')

        debugRemoteType = ET.SubElement(attributes, 'attribute')
        debugRemoteType.set('datatype', 'string')
        debugRemoteType.set('name', 'DebugRemoteType')
        debugRemoteType.text = '"{}"'.format(
            record['Attributes']['DebugRemoteType'])

        debugWireType = ET.SubElement(attributes, 'attribute')
        debugWireType.set('datatype', 'string')
        debugWireType.set('name', 'DebugWireType')
        debugWireType.text = '"{}"'.format(
            record['Attributes']['DebugWireType'])

        if record['Attributes']['TypeIsVarchar']:
            typeIsVarchar = ET.SubElement(attributes, 'attribute')
            typeIsVarchar.set('datatype', 'string')
            typeIsVarchar.set('name', 'TypeIsVarchar')
            typeIsVarchar.text = '"{}"'.format(
                str(record['Attributes']['TypeIsVarchar']).lower())

    aliases = ET.SubElement(dataSource, 'aliases')
    aliases.set('enabled', 'yes')

    sortedRecords = sorted(tableMetadata.ColumnMetadata,
                           key=lambda x: x['RemoteName'])

    for record in sortedRecords:
        if record['RemoteName'].isdigit():
            continue

        column = ET.SubElement(dataSource, 'column')

        if record['Aggregation'] == 'Hour':
            column.set('aggregation', 'Hour')

        captionParts = re.split(r'(\d+)', record['RemoteName'])
        realCaption = ''

        for captionPart in captionParts:
            if not captionPart.strip():
                continue

            realCaption = realCaption + captionPart.capitalize()

        chars = re.escape(string.punctuation)
        realCaption = re.sub(r'['+chars+']', ' ', realCaption).title()

        column.set('caption', realCaption)
        column.set('datatype', record['LocalType'])
        column.set('name', '[{}]'.format(record['RemoteName']))
        column.set(
            'role', 'measure' if record['LocalType'] in measureTypes else 'dimension')
        column.set('type', 'quantitative' if record['LocalType'] in quantitativeTypes else (
            'ordinal' if record['LocalType'] in ordinalTypes else 'nominal'))

    numRecordsColumn = ET.SubElement(dataSource, 'column')
    numRecordsColumn.set('datatype', 'integer')
    numRecordsColumn.set('name', '[Number of Records]')
    numRecordsColumn.set('role', 'measure')
    numRecordsColumn.set('type', 'quantitative')
    numRecordsColumn.set('user:auto-column', 'numrec')

    numRecordsCalculation = ET.SubElement(numRecordsColumn, 'calculation')
    numRecordsCalculation.set('class', 'tableau')
    numRecordsCalculation.set('formula', '1')

    layout = ET.SubElement(dataSource, 'layout')
    layout.set('dim-ordering', 'alphabetic')
    layout.set('dim-percentage', '0.5')
    layout.set('measure-ordering', 'alphabetic')
    layout.set('measure-percentage', '0.4')
    layout.set('show-structure', 'true')

    semanticValues = ET.SubElement(dataSource, 'semantic-values')

    semanticValue = ET.SubElement(semanticValues, 'semantic-value')
    semanticValue.set('key', '[Country].[Name]')
    semanticValue.set('value', '"United States"')

    xmlData = ET.tostring(dataSource)
    reparsedXmlData = minidom.parseString(xmlData)

    tdsFile = open(filePath, "wb")
    tdsFile.write(reparsedXmlData.toprettyxml(indent="  ", encoding='utf-8'))


def publishDataSource(filePath, projectId):
    tableauAuth = TSC.TableauAuth(tableauUserId, tableauPassword)
    server = TSC.Server(tableauServerAddress)

    if tableauIgnoreSslCert:
        server.add_http_options({'verify': False})

    server.auth.sign_in(tableauAuth)

    datasourceCredentials = TSC.ConnectionCredentials(
        name=pgUser, password=pgPassword)

    datasource = TSC.DatasourceItem(projectId)

    if useCustomSQL:
        datasource.name = dataSourceName

    datasource = server.datasources.publish(
        datasource, filePath, 'CreateNew', datasourceCredentials)

    if verbose:
        print('Published datasource {} to Tableau Server running at {}'.format(
            datasource.name, tableauServerAddress))
        lineClear()

    server.auth.sign_out()


if __name__ == '__main__':
    generateTdsFiles()
