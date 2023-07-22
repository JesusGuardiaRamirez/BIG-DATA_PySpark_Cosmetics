# Analisis con PySpark - BigData

![Captura de pantalla 2023-07-22 a las 21 14 23](https://github.com/JesusGuardiaRamirez/SYL/assets/125477881/21c1e4f9-9715-4c1b-8201-39aa8017a050)



## Que es PySpark??


PySpark es una biblioteca de Python que proporciona una API para interactuar con Apache Spark, un potente motor de procesamiento distribuido de big data. Spark está diseñado para realizar operaciones de procesamiento y análisis de datos a gran escala y en paralelo, lo que permite manejar grandes volúmenes de información de manera eficiente.

Las principales características y conceptos clave de PySpark son:

Apache Spark: Es un framework de código abierto para procesamiento de datos en clústeres de computadoras. Spark está escrito en Scala y ofrece interfaces para Java, Python (PySpark), R y otros lenguajes.

Procesamiento en Memoria: Spark realiza la mayoría de sus operaciones en memoria, lo que lo hace significativamente más rápido que los sistemas que dependen únicamente del acceso a disco.

Resilient Distributed Datasets (RDD): Es el principal concepto de datos en Spark. Un RDD es una colección distribuida e inmutable de objetos que se puede procesar en paralelo.

Transformaciones y Acciones: Spark proporciona operaciones de transformación y acción. Las transformaciones (como map, filter y groupBy) generan nuevos RDD a partir de uno existente, mientras que las acciones (como count, collect y save) devuelven resultados o escriben datos en disco.

Computación Distribuida: Spark permite realizar cómputos distribuidos y paralelos en clústeres de computadoras, lo que permite escalar el procesamiento para grandes volúmenes de datos.

Integración con Diferentes Fuentes de Datos: PySpark ofrece conectores para leer y escribir datos desde y hacia diversas fuentes, como Hadoop Distributed File System (HDFS), bases de datos SQL, Amazon S3, Apache Hive, entre otros.

Machine Learning y Procesamiento de Gráficos: Spark también proporciona bibliotecas y módulos para realizar tareas de machine learning y procesamiento de gráficos de manera distribuida.

PySpark es especialmente útil cuando se trabaja con grandes conjuntos de datos que exceden la capacidad de una sola máquina. Al aprovechar la capacidad de procesamiento distribuido de Spark, PySpark permite realizar análisis de big data y procesamiento de datos a gran escala de manera eficiente y efectiva.





# La página web donde he cogido los datos para su analisis está en el siguiente enlace, donde te llevará directo.



[DATASET](https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop)



# Empezamos con el análisis. :heavy_exclamation_mark:

### :pushpin: Para ello, importamos las librerias necesarias para poder ejecutar el codigo:

      from pyspark.sql import SparkSession
      from pyspark.sql.types import StructType, StructField, IntegerType, StringType
      import numpy as np
      import time
      from pyspark.sql.functions import col


### :pushpin: Incializamos Spark definiendolo en la variable (df).

![Captura de pantalla 2023-07-22 a las 21 54 50](https://github.com/JesusGuardiaRamirez/SYL/assets/125477881/301f2d24-2c0c-4843-ac0b-f57e9fe2bf50)

Utilizamos .count() para ver el numero de filas que tienen, aparecen mas de 20 millones de registros, (BigData). Los archivos CSV, son desde el mes de Octubre del 2019 a Enero de 2020. (5 en total)

![Captura de pantalla 2023-07-22 a las 21 49 20](https://github.com/JesusGuardiaRamirez/SYL/assets/125477881/70088877-62db-4684-b8f5-48da390e597d)



Vemos que en primer lugar aparece el nombre de las columnas, en segundo lugar el tipo de dato que son y por ultimo si tienes tiene nulos o no. 



### :pushpin: Usamos .select para ver que valores tiene la columna (event_type) y utilizamos .distinct para ver solamente los únicos. 


![Captura de pantalla 2023-07-22 a las 22 41 40](https://github.com/JesusGuardiaRamirez/SYL/assets/125477881/c191216d-31fc-481e-983b-25965eb3ebcb)


      Purchase (Compra)
      View (Visitas)
      Cart (Añadido al carrito)     
      Remove_from_cart (Quitado del carrito)



### :pushpin: Hacemos lo mismo con la columna de (brand) para ver algunas de las marcas con las que vamos a trabajar, aunque solo enseña los 20 primeros.


![Captura de pantalla 2023-07-22 a las 22 52 48](https://github.com/JesusGuardiaRamirez/SYL/assets/125477881/173a30f7-1323-42ae-ba3e-004565382da6)


### :pushpin: Codeamos lo siguiente para ver los productos que se han llegado a meter en el carrito de la compra, por su (product_id):

      
```python
\df.select(["product_id"]).filter("event_type ='cart'").show()
\in_cart = df.select(["product_id"]).filter("event_type ='cart'").count()
\print(in_cart)
```


![image](https://github.com/JesusGuardiaRamirez/SYL/assets/125477881/0b8f2609-7dc3-464d-bf66-21e62c2d0767)



Hay mas de 5 millones de productos que fueron seleccionados y enviados a el carro para su compra.
