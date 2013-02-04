clean:
	rm -f *~
	rm -f *.pyc

tgz:
	tar cvfz greenhadoop.tgz *.py *.sh Makefile README algorithm.txt greenhadoop.patch greenavailability/*.py 
