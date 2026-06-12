# Variables con los argumentos por defecto para la ejecución
SUBSCRIPTIONS = data/local_subscriptions.json
TOP_K = 50

# Regla por defecto (la que se ejecuta al escribir solo 'make')
all: run

# Regla para compilar y ejecutar todo junto con un único comando
run:
	sbt "run --subscription-file $(SUBSCRIPTIONS) --top-k $(TOP_K)"

# Regla por si solo quieren compilar el proyecto sin ejecutarlo
compile:
	sbt compile

# Regla para limpiar los archivos temporales de sbt y la compilación
clean:
	sbt clean
	rm -rf target/ project/target/ project/project/

.PHONY: all run compile clean