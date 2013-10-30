"""

Markov Chain method of computing relations between items in ranked lists. 

This application generates a datastore of ranked markov data as lists. The
algorithm below allows data to be continuously fed to the datastore for 
real time applications or growing datasets.

2013 - Henry Hammond

This code is available for anyone to use and expand.

"""


import sqlite3

# Simple SQLite3 datastore module for markov relations
class DataModule:

	# create database
	def __init__(self,database):
		self.db = database

		self.conn = sqlite3.connect(database)
		self.conn.text_factory = str
		self.c = self.conn.cursor()

		self.create_tables()

	"""
		Create tables for the database. 

		The first table stores unique elements while the
		second stores relations between elements.
	"""
	def create_tables(self):
		# clear old db
		self.c.execute("DROP TABLE IF EXISTS elements")
		self.c.execute("DROP TABLE IF EXISTS relations")
		
		# create new tables

		# main table
		self.c.execute('''
		CREATE TABLE elements(
			class		TEXT UNIQUE,	-- description of item
			id  		INTEGER PRIMARY KEY AUTOINCREMENT
		);
		''')

		# relationship table
		self.c.execute('''
		CREATE TABLE relations(
			class				TEXT,		-- description of item
			id  				INTEGER PRIMARY KEY AUTOINCREMENT,
			count 				INTEGER,	-- number of times this relation occures
			rating_sum			REAL,		-- sum of rankings of this relation
			rating_sum_squares	REAL,		-- sum of squares used for varience calculations
			parent_id			INTEGER,	-- foreign key to parent
			FOREIGN KEY(parent_id) REFERENCES elements(id)
		);
		''')

		# create an index to speed things up
		self.c.execute("CREATE INDEX relations_index ON relations(parent_id);")
		self.conn.commit()

	def existsRootElement(self,name):
		self.c.execute("SELECT * FROM elements WHERE class=(?)",(name,))
		check = self.c.fetchone()

		return check is not None

	def getRootElementId(self,name):

		# name = u"'%s'"%name
		self.c.execute("SELECT id FROM elements WHERE class=(?)",(name,))
		
		try:
			return int(self.c.fetchone()[0])
		except:
			return None

	def getChildren(self,parent):
		self.c.execute('''
			SELECT
				relations.class, 
				relations.count, 
				relations.rating_sum,
				relations.rating_sum_squares
			FROM 
				relations 
			JOIN
				elements 
			ON 
				relations.parent_id=elements.id 
			WHERE 
				elements.class=(?)
			'''
			,(parent,) )
		return self.c.fetchall()

	def existsChildElement(self,parent,child):
		parent_id = self.getRootElementId(parent)
		self.c.execute("SELECT * FROM relations WHERE class=? AND parent_id=?",(child,parent_id))

	def addRootElement(self,name):
		self.c.execute("INSERT INTO elements VALUES (?,NULL)", (name,) )

	def addRelatedElement(self,parent,name,count,rating):
		parent_id = self.getRootElementId(parent)
		self.c.execute("INSERT INTO relations VALUES (?,NULL,?,?,?,?)",(name,count,rating,rating*rating,parent_id))

	def getChildId(self,parent,child):
		self.c.execute('''
			SELECT 
				relations.id 
			FROM 
				relations 
			JOIN 
				elements 
			ON 
				relations.parent_id=elements.id 
			WHERE 
				relations.class=(?) AND
				elements.class=(?)
			
			'''
			,(child,parent) )

		try:
			return int(self.c.fetchone()[0])
		except:
			return None

	def updateChild(self,parent,child,count,rating):
		parent_id = self.getRootElementId(parent)
		child_id  = self.getChildId(parent,child)

		self.c.execute('''
			UPDATE
				relations
			SET 
				count=(?),
				rating_sum=(?),
				rating_sum_squares=(?)
			WHERE 
				id=(?) 
			AND
				parent_id=(?)
			;
			''',(count,rating,rating*rating,child_id,parent_id))

	def updateChildCount(self,parent,child,increment):
		parent_id = self.getRootElementId(parent)
		child_id = self.getChildId(parent,child)

		self.c.execute('''
			UPDATE
				relations
			SET
				count=count+(?)
			WHERE
				id=(?) 
			AND
				parent_id=(?)
			;
			''',(increment,child_id,parent_id))

	def updateChildRating(self,parent,child,rating):
		parent_id = self.getRootElementId(parent)
		child_id = self.getChildId(parent,child)

		self.c.execute('''
			UPDATE
				relations
			SET
				rating_sum=rating_sum+(?),
				rating_sum_squares=rating_sum_squares+(?)
			WHERE
				id=(?) 
			AND
				parent_id=(?)
			;
			''',(rating,rating*rating,child_id,parent_id))		

	def incrementChild(self,parent,child,increment,rating):
		parent_id = self.getRootElementId(parent)
		child_id = self.getChildId(parent,child)

		self.c.execute('''
			UPDATE
				relations
			SET
				count=count+(?),
				rating_sum=rating_sum+(?),
				rating_sum_squares=rating_sum_squares+(?)
			WHERE
				id=(?) 
			AND
				parent_id=(?)
			;
			''',(increment,rating,rating*rating,child_id,parent_id))

	def computeRating(self,element):
		self.c.execute('''
		SELECT
			sum(rating_sum)/sum(count) 
		FROM 
			elements 
		JOIN 
			relations 
		ON 
			elements.id=relations.parent_id
		WHERE 
			elements.class=(?)
		;
		''',(element,))


	def commit(self):
		self.conn.commit()



class MarkovRatingSystem:

	# create new ranking system with database module
	def __init__(self,datamodule):
		self.datastore = datamodule

	"""
	Feed a line of data into this system.

	Note: repeated elements will be counted for each repetition in the line
	"""
	def feedLine(self, rating, relationList):

		# iterate through items in list
		for x in relationList:

			# check add to datastore
			if not self.isRootElement(x):
				self.addNewRootElement(x)

			# iterate through elements in list and update relations
			for y in relationList:
				# note that relations are reflexive
				if x != y:

					if not self.isChildOf(x,y):
						self.addRelatedElement(x,y,1,rating)
					else:
						self.incrementRelatedBoth(x,y,1,rating)

	# check if element in datastore
	def isRootElement(self,element):
		return self.datastore.existsRootElement(element)

	# add new element to datastore
	def addNewRootElement(self,element):
		self.datastore.addRootElement(element)

	# check if one item is related to another as a child
	def isChildOf(self,element,relatedElement):
		childlist = self.datastore.getChildren(element)
		names = [ x[0] for x in childlist ]
		return relatedElement in names

	# add new child element
	def addRelatedElement(self,parent,childName,childCount,childRating):
		self.datastore.addRelatedElement(parent,childName,childCount,childRating)

	# increment count of child
	def incrementRelatedElementCount(self,parent,childName,increment):
		self.datastore.updateChildCount(parent,childName,increment)

	# update rating of child
	def updateRelatedElementRating(self,parent,childName,rating):
		self.datastore.updateChildRating(parent,childName,rating)

	# update both rating and count
	def incrementRelatedBoth(self,parent,childName,increment,rating):
		self.datastore.incrementChild(parent,childName,increment,rating)

	# commit datastore changes
	def commit(self):
		self.datastore.commit()


def test1():

	testDataSet = [

	[1,'a','b','c'],
	[3,'a','d'],
	[8,'d','b'],
	[2,'d','b','c','e'],
	[10,'a','b'],

	]

	datastore = DataModule('test.db')
	markov = MarkovRatingSystem(datastore)

	for line in testDataSet:
		markov.feedLine( line[0],line[1:] )
	datastore.commit()

	print 'a',datastore.getChildren('a')
	print 'b',datastore.getChildren('b')
	print 'c',datastore.getChildren('c')
	print 'd',datastore.getChildren('d')
	print 'e',datastore.getChildren('e')

if __name__=='__main__':

	test1()
