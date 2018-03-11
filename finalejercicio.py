# It works in Python 2.7 as well as Python3.5
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
    #En este primer mapper desmenuzamos cada linea para que nos devuelva en una
    #lista (por cada linea) lo que nos interesa. IP de clave,
    def tf_mapper(self, _, line):
        line = line.split()
	#Comando utilizado para obtener cada conjunto de valores separados por espacios
        if len(line)>=2:
            IP=line[0]
            FECHA=line[3]
            Archivo=line[6]
            Exito=line[8]
            if (Exito=='200'):
		# Con este codigo convierto la fecha del formato que me la han dado dia/mes/ano:horas:minutos:segundos al tiempo
		# que ha pasado desde el "comienzo de la cuenta de los computadores" (alrededor de 1970) hasta ahora
                fecha= time.mktime(time.strptime(FECHA,'[%d/%b/%Y:%H:%M:%S'))
		#'''Devuelvo IP, la fecha a la que se ha accedido a determinado archivo y el archivo'''
                yield IP,(fecha,Archivo)

    #Primer reducer, en el que juntaremos los valores de las sesiones
    def tf_reducer(self,IP,values):
        T=60*60 #defino T como el limite de tiempo entre sesiones. En este caso, lo fijamos en 1h
        hora=0
	# Contador que utilizaremos para definir el tiempo que ha pasado
	# desde el ultimo acceso a internet.
        i=0 #Contador
        horas=[]
        archivos=[]
        for fecha,archivo in values:            
	    #Filtro de este 
            if (fecha-hora>=0 and fecha-hora<T):
		# Si ha transcurrido menos de una hora desde la primera consulta, añado el archivo visitado a la lista de 
		# listas de archivos vistados, en la posicion i (que llevamos en el contador i, que nos marca la
		# sesion en la que nos encontramos)
                archivos[i-1].append(archivo)
            else:
                i+=1
                hora=fecha
                horas.append([hora])
                archivos.append([archivo])                    
        for v in range(len(archivos)):
            yield IP,(horas[v][0],list(set(archivos[v])),i)

    #vemos los tiempos entre sesiones, para decir la frecuencia con la que se suele conectar
    def time_between_conn(self,key,values):
        fecha=0
        diferencia_fecha=0
        for FECHA,lista,conn_realizadas in values:
            diferencia_fecha=FECHA-fecha
            fecha=int(FECHA)
            yield lista,(key,diferencia_fecha,FECHA,conn_realizadas,1)
     
    #IMPORTANTE:
    #-----------
    #La clave es la lista de archivos visitados.
    def comport_repetidos(self,key,values):
	#Creo una lista donde iré guardando las IP's que tengan el mismo comportamiento
        LISTA_COMPORTAMIENTOS=[]
	#genero un contador
        i=0
        for (IP,time_between_conn,FECHA,conn_realizadas,repeticion_en_sesion) in values:
            if (not (IP in LISTA_COMPORTAMIENTOS)):
		#en cada vuelta, añado la IP a la lista
                LISTA_COMPORTAMIENTOS.append(IP)
                i=i+1
            else:
                i=i+1
	    #y en cada vuelta devuelvo esa lista. Devuelvo como clave la IP y la lista de archivos visitados.
	    #Como valores: 
            #		la fecha en segundos, 
	    #  		el numero de sesiones en el día y 
	    #		si se ha repetido el comportamiento alguna vez en general
            #		la lista de los usuarios que han repetido ese comportamiento
            yield (IP,key),(FECHA,time_between_conn,conn_realizadas,i-1,LISTA_COMPORTAMIENTOS)
            
    def agrupaciones(self,key,values):
        I=0	
        for FECHA,time_between_conn,conn_realizadas,numero,LISTA in values:
            I=I+1
	#En este programa devuelvo lo mismo que en el anterior, solo que ahora
	#el valor I medirá las veces que el usuario j (por ejemplo) visita el
	#archivo k. Antes, "numero" nos devolvia el numero de repeticiones en
	#general. Al recorrer todos los archivos con la misma IP con el contador,
	#es sencillo contar las repeticiones de cada usuario
        yield key[1],(key[0],conn_realizadas,I,LISTA)

    def comportamientos(self,behaviour,values):
	#Este set lo voy a utilizar para guardar todos los usuarios que visiten
	#un determinado archivo, sin repeticiones, ordenado
        user_list=set()
        USER=[]
        T_S=[]
        N_B=[]
	#Para cada valor que nos den en values
        for user,t_s,n_b,lista_usuarios in values:
	    #para cada valor de la lista lista_usuarios
            for i in range(len(lista_usuarios)):
                user_list.add(lista_usuarios[i])
            USER.append(user)
            T_S.append(t_s)
            N_B.append(n_b)
        for i in range(len(USER)):
	    #fin.
            yield 'RESULTADOS-->',(USER[i],T_S[i],behaviour,N_B[i],list(user_list))
    
    #Este programa lo he utilizado en otro apartado, pero ahora resulta inutil.
    #'''def filtro_comportamientos(self,key,values):
    #    LISTA=[]
    #    TODO=[]
    #    esta=False
    #    for (lista,diferencia_fecha,FECHA,conn_realizadas,j) in values:
    #        for i in range(len(lista)):
    #            for j in range(len(LISTA)):
    #                if (not(lista[i] in LISTA[j])):
    #                    esta=False
    #                else:
    #                    esta=True
    #        if (not esta):
    #            LISTA.APPEND(lista)
    #        if diferencia_fecha>60*60:
    #            yield key,(lista,diferencia_fecha,FECHA,conn_realizadas,sum(j))'''


    
    def steps(self):
        return [
	    # ordenamos con cuidado los mapper y los reducer. Con cuidado de no devolver en un
	    # mapper una IP cuando en el siguiente reducer deberia recoger una lista
            MRStep(mapper = self.tf_mapper,
                   reducer = self.tf_reducer),
            MRStep(reducer = self.time_between_conn),
            MRStep(reducer=self.comport_repetidos),
            MRStep(reducer=self.agrupaciones),
            MRStep(reducer=self.comportamientos)
        ]

if __name__ == '__main__':
    MRTrabajo.run()
