# Parks Collector


[![Python](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](#)

Proyecto para recopilar datos de tiempos de espera en parques de atracciones de Europa y otros continentes usando la API de ThemeParks.wiki.

---

## 🗂️ Estructura del proyecto



Tras un par de años estudiando programación y buscando cómo orientarme hacia el campo del análisis de datos apareció en mi cabeza la idea de recopilar y tratar los datos de uno de mis pasatiempos favoritos, los parques de atracciones. Queriendo unir ambas cosas nace la idea de Parks Collector.

![giphy](https://github.com/user-attachments/assets/88a0e783-b1ab-4fc7-a35a-585946c6fa7a)

## 🎡 Descripción de Parks Collector 🎢

Parks Collector es la primera pieza de un puzzle que todavía hoy (enero de 2026) parece lejos de completarse. Parks Collector es un, no demasiado complejo, script que recoge los datos en tiempo real de casi medio centenar de parques temáticos repartidos por todo el mundo con el objetivo de poder obtener tendencias temporales, comparaciones con números de visitantes, efectos estacionales o simplemente encontrar la mejor hora del día para subir a Space Mountain sin tener que aguantar dos horas de espera.

## Estado del proyecto

Desde que arrancó este proyecto en octubre de 2025 hasta el momento que escribo estas lineas, las ideas, el código, la lista de parques a analizar y otros muchos detalles no han dejado de variar. Por este motivo creo que es importante dejar claro que Parks Collector es parte de un

<h3 align="center">🚧PROYECTO EN CONSTRUCCIÓN🚧</h3>

# Funciones de Parks Collector 〽️

Como se comenta anteriormente la principal función de este script no es otra que recopilar los datos de espera en tiempo real de las diferentes atracciones de cada uno de los parques temáticos elegidos. Pero para llegar al punto actual he tomado algunas decisiones sobre las que hablaré en este punto.

  🛠️📡 **Conexión con la API.** Dado que ya existen ciertas interfaces que muestran en tiempo real las esperas de parques de todo el mundo yo decidí utilizar la de [themeparks.wiki](https://themeparks.wiki/) que cuenta con un buen número de parques repartidos por 5 continentes

  🛠️🕐 **Múltiples husos horarios.** La ubicación de los diferentes parques hace que vayamos a trabajar con diferentes husos horarios. Para que los horarios de cada parque aparezcan siempre referenciados con su horario local he utilizado la librería [PYTZ](https://pypi.org/project/pytz/)

  🛠️🗺️ **Recopilación de datos globales** Aunque la primera versión de Parks Collector se reducía a parques europeos, el alcance del script se ha aumentado hasta recoger datos de parques ubicados en 4 continentes diferentes. Europa, Asia, Oceanía y América son los continentes de los que se recopilan datos.

  ## Próximos objetivos 

  🎯 **Mejorar la gestión de logs**
  
  🎯 **Introducir uso de Pandas**
  
  🎯 **Construcción base de datos**
  
  🎯 **Creación de app en Flutter**

![giphy](https://github.com/user-attachments/assets/98e6590c-540f-41f1-98b3-62d33ea537d4)

##Como ejecutarlo

Muy sencillito para que puedas arrancar este script en tu propio sistema.

```
bash
pip install -r requirements.txt
python collector.py
```


## Acceso al proyecto y licencia ✍️

Este proyecto nace de mi inexperiencia y ganas de aprender de los problemas que pueden surgir en un proyecto real (y con los que ya me estoy encontrando). Por este motivo te doy acceso total a mi trabajo y puedes descargarlo, modificarlo, compartirlo y hasta escribir con las mejoras que se te ocurran.

  
