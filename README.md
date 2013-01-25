CSVInputFormat
==============

Input format for hadoop able to read multiline CSVs

Run BasicTest.java to see it working. Check src/test/resource/test.csv to see a multiline demofile.

The key returned is the line number and the value is a List with the column values

Example:
--------------------------------------------------------------------------------
If we read this CSV (note that line 2 is multiline):

	Name, address, email
	Joe Demo,"2 Demo Street, 
	Demoville, 
	Australia. 2615",joe@someaddress.com
	Jim Sample,,jim@sample.com
	Don Joe,joe@sample.com
	Jack Example,"1 Example Street, Exampleville, Australia. 2615",jack@example.com


The output is as follows:


	==> TestMapper
	==> key=0
	==> val[0] = Name
	==> val[1] =  address
	==> val[2] =  email
	
	==> TestMapper
	==> key=1
	==> val[0] = Joe Demo
	==> val[1] = 2 Demo Street, 
	Demoville, 
	Australia. 261
	==> val[2] = joe@someaddress.com
	
	==> TestMapper
	==> key=2
	==> val[0] = Jim Sample
	==> val[1] = 
	==> val[2] = jim@sample.com
	
	==> TestMapper
	==> key=3
	==> val[0] = Don Joe
	==> val[1] = joe@sample.com
	
	==> TestMapper
	==> key=4
	==> val[0] = Jack Example
	==> val[1] = 1 Example Street, Exampleville, Australia. 261
	==> val[2] = jack@example.com