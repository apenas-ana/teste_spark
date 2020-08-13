"""
A abordagem abaixo, utilizando RDD ao invés do Dataframe, começou a dar um erro de 'list index out of range', o qual não consegui corrigir.
Então como estava levando muito tempo nesta abordagem, fiz utilizando Dataframe (arquivo NasaFileAnalysis.py).
"""

from operator import add

textFile = sc.textFile("teste_semantix/access_log_Jul95, teste_semantix/access_log_Aug95")

lines = textFile.flatMap(lambda line: line.split("\n"))

lines.count()

lines_splited = lines.map(lambda row: row.split(" "))

pre_hosts = lines_splited.map(lambda row: (row[0], 1))

hosts = pre_hosts.reduceByKey(add)

uniqueHostsCount = hosts.filter(lambda tup: tup[1] == 1)

# total de hosts únicos
uniqueHostsCount.count()

# total de erros 404
err404 = lines.map(lambda line: ' 404 ' in line)

err404.count()

# outra abordagem para obter total de erros 404
err = rdd.filter(lambda x: x[8] == '404').map(lambda x: (x[0], 1))
err = err.reduceByKey(add)
