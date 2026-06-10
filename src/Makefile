#
#  This Makefile is designed to compile the starter code and create 4700recv and 4700send.
#  Usage:
#       make gson.jar  # download GSON jar (Optional)
#       make 
#
#  Students must submit a separate Makefile with their submission that compiles 
#  their code. Whether the '4700recv' and '4700send' script is created by the Makefile is 
#  the developer's decision. 
#

JFLAGS = -g -cp .:gson.jar
JC = javac
JAR = jar
JAVA = java

CLASSES = Sender.java Receiver.java
JAR_SEND = sender4700.jar
JAR_RECV = receiver4700.jar

# NOTE:
# The starter code uses the GSON library for JSON serialization and deserialization.
# You can replace this with another JSON library of your choice, such as org.json or Jackson.
# Ensure that your chosen library correctly encodes and decodes messages while maintaining
# the expected structure required by the simulator.
#
# By uncommenting the line below, you acknowledge the starter code uses GSON and that
# you can modify it to use the JSON library of your choice.
#GSON_URL = https://repo1.maven.org/maven2/com/google/code/gson/gson/2.10.1/gson-2.10.1.jar

default: build

gson.jar:
	wget -O gson.jar $(GSON_URL)

%.class: %.java
	$(JC) $(JFLAGS) $<

build: gson.jar $(CLASSES:.java=.class) 
	@echo "Creating manifests"
	echo "Main-Class: Sender\nClass-Path: gson.jar" > manifest_sender.txt
	echo "Main-Class: Receiver\nClass-Path: gson.jar" > manifest_receiver.txt

	@echo "Building JARs"
	$(JAR) cfm $(JAR_SEND) manifest_sender.txt *.class
	$(JAR) cfm $(JAR_RECV) manifest_receiver.txt *.class

	@echo "Creating executables"
	echo '#!/bin/bash\n$(JAVA) -jar $(JAR_SEND) "$$@"' > 4700send
	echo '#!/bin/bash\n$(JAVA) -jar $(JAR_RECV) "$$@"' > 4700recv
	chmod +x 4700send 4700recv

clean:
	@echo "Cleaning"
	rm -f *.class manifest_*.txt receiver4700.jar sender4700.jar 4700send 4700recv

