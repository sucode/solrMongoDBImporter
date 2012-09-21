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
```xml
<dataConfig>
	<dataSource name="mongod" type="MongoDataSource" host="127.0.0.1" port="27017" database="example" />
</dataConfig>
```

### Entity config
```xml
<entity processor="MongoDBEntityProcessor" dataSource="mongod" name="test" collection="coll">
	<field column="_id" name="docId"/>
	<field column="title" name="title"/>
	<!-- other fileds -->
</entity>
```xml

### ObjectId transformer
Somethime we need a Long docId, but we have ObjectId in MongoDB, so a transformer may help.
This transfomer just cover the ObjectId to it's hashcode :-)
```xml
<entity processor="MongoDBEntityProcessor" dataSource="mongod" name="test" collection="coll">
	<field column="_id" name="docId" hashObjectId="true"/> <!-- docId has long type-->
	<field column="title" name="title"/>
	<!-- other fileds -->
</entity>
```xml

### Put it together
```xml
<dataConfig>
	<dataSource name="mongod" type="MongoDataSource" host="127.0.0.1" port="27017" database="example" />
	<entity processor="MongoDBEntityProcessor" dataSource="mongod" name="test" collection="coll">
		<field column="_id" name="docId" hashObjectId="true"/> <!-- docId has long type-->
		<field column="title" name="title"/>
		<!-- other fileds -->
	</entity>
</dataConfig>
```

