all: data.csv cliff/Bandwidth.class

%.class: %.java
	javac $<

data.csv: cliff/Gen.class
	java -cp cliff Gen 5000000 > $@

clean:
	@rm -f cliff/*.class
