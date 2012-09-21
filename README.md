# Solr MongoDB Importer
Thi is a MongoDB Data Importer for Solr.

## Features
* import data from MongoDB to solr
* support full import and delta import 
* support for transform from MongoDB's ObjectID to Long

## Dependencies
* mongo-java-driver

## Usage

### DataSource config
* support config items
	1. host (default: localhost)
	1. port (default: 27017)
	1. database
	1. username (optional)
	1. password (optional)

```xml
<dataConfig>
	<dataSource name="mongod" type="MongoDataSource" host="127.0.0.1" port="27017" database="example" />
</dataConfig>
```

### Entity config
* support config items
	1. collection
	1. command
	1. deltaCommand (optional, if not set, and request a delta import, we will use command instead)
	
```xml
<entity processor="MongoDBEntityProcessor" dataSource="mongod" name="test" collection="coll" command="{}">
	<field column="_id" name="docId"/>
	<field column="title" name="title"/>
	<!-- other fileds -->
</entity>
```

### ObjectId transformer
Somethime we need a Long docId, but we have ObjectId in MongoDB, so a transformer may help.

This transfomer just cover the ObjectId to it's hashcode :-)

```xml
<entity processor="MongoDBEntityProcessor" dataSource="mongod" name="test" collection="coll" command="{}">
	<field column="_id" name="docId" hashObjectId="true"/> <!-- docId has long type-->
	<field column="title" name="title"/>
	<!-- other fileds -->
</entity>
```

### Put it together
```xml
<dataConfig>
	<dataSource name="mongod" type="MongoDataSource" host="127.0.0.1" port="27017" database="example" />
	<entity processor="MongoDBEntityProcessor" dataSource="mongod" name="test" collection="coll" command="{}">
		<field column="_id" name="docId" hashObjectId="true"/> <!-- docId has long type-->
		<field column="title" name="title"/>
		<!-- other fileds -->
	</entity>
</dataConfig>
```

