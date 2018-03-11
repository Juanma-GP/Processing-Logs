# -*- coding: utf-8 -*-

# Data from http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
# First field ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
# Second field ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
# Beware. There are corrupt lines. First 100.000 lines in Jul95 are without mistakes.
# You have to install mrjob in Python. You can do it with PIP
from mrjob.job import MRJob
from mrjob.step import MRStep
import time

class MRTrabajo(MRJob):
    SORT_VALUES = True
    def tf_mapper(self, _, line):
        line = line.split()
        if len(line)>=2:
            IP=line[0]
            FECHA=line[3]
            Archivo=line[6]
            Exito=line[8]
            if (Exito=='200'):
                fecha= time.mktime(time.strptime(FECHA,'[%d/%b/%Y:%H:%M:%S'))
                '''Devuelvo IP, la fecha a la que se ha accedido a determinado archivo, asi como el archivo'''
                yield IP,(fecha,Archivo)

    #Primer reducer, en el que juntaremos los valores de las sesiones
    def tf_reducer(self,IP,values):
        T=60*60 #defino T como el limite de tiempo entre sesiones. En este caso,
        #lo fijamos en 1h
        hora=0
        i=0 #Contador
        horas=[]
        archivos=[]
        for fecha,archivo in values:            
            #Filtro de este 
            if (fecha-hora>=0 and fecha-hora<T):
                archivos[i-1].append(archivo)
            else:
                hora=fecha
                horas.append([hora])
                i=i+1
                archivos.append([archivo])
        for v in range(len(archivos)):
            yield (IP,list(set(archivos[v]))),(horas[v][0],i)
    def reducido(self,key,values):
        hora=0
        T=385000
        for horas,n in values:
            if horas-hora>T:
                hora=horas
                yield key[0],(key[1],horas,n)
    def steps(self):
        return [
            MRStep(mapper = self.tf_mapper,
                   reducer = self.tf_reducer),
            MRStep(reducer = self.reducido)
        ]


if __name__ == '__main__':
    MRTrabajo.run()
