# Guion del video (≈ 6 minutos)

Antes de grabar: `docker compose up -d` ya ejecutado y la interfaz de Flink abierta en <http://localhost:8081>. El pipeline tarda unos 4 minutos; podés cortar esa espera en la edición.

## Escena 1 — El problema (30 s)
**EN PANTALLA:** `docs/diagrama_arquitectura.png`.
**DECIR:** «Soy Nicolás Blanco. Mi TP calcula la velocidad promedio de cada moto de una empresa de delivery, minuto a minuto. El problema es que los celulares pierden señal, reenvían datos y mandan pings atrasados. El pipeline tiene que usar cuándo ocurrió cada ubicación y no contar dos veces un ping repetido.»

## Escena 2 — La arquitectura (45 s)
**EN PANTALLA:** el diagrama, señalando cada caja.
**DECIR:** «Un productor simula cinco motos y escribe en Kafka, en el tópico `flota.ubicaciones`, con clave `vehicle_id`, para que los pings de cada moto queden en orden en la misma partición. Apache Beam, ejecutado sobre Flink, lee con KafkaIO, valida, usa el tiempo de evento, agrupa en ventanas de un minuto, elimina duplicados y calcula el promedio. Los resultados van a `flota.metricas` con la clave moto más ventana, y los inválidos a `flota.invalidos`.»

## Escena 3 — El productor (45 s)
**EN PANTALLA:** terminal: `docker compose run --rm app python -m flota.productor`.
**DECIR:** «El productor manda un ping cada cinco segundos por moto durante tres minutos. Además mete a propósito un duplicado, un evento desordenado, uno tardío, uno demasiado tardío y uno inválido. Todo lo que envía queda registrado para comprobar después el resultado.»

## Escena 4 — El pipeline en Flink (60 s)
**EN PANTALLA:** terminal: `docker compose run --rm pipeline`; después, la interfaz de Flink con el job `flota-metricas` y sus etapas.
**DECIR:** «El pipeline se envía a Flink. Acá se ven las etapas: la lectura de Kafka, la validación con las ventanas, la deduplicación con estado y el promedio por moto. Lee lo que hay en el tópico, lo procesa y termina.»

## Escena 5 — El resultado y la verificación (75 s)
**EN PANTALLA:** terminal: `docker compose run --rm app python -m flota.consumidor`; señalar la tabla y las líneas `[OK]`.
**DECIR:** «El consumidor lee los resultados y se queda con el último valor de cada moto y minuto. Las 21 ventanas coinciden con lo enviado. El duplicado se contó una sola vez: la ventana tiene 12 pings, no 13. El evento desordenado y el tardío quedaron en el minuto en que ocurrieron, no en el que llegaron. El inválido está en `flota.invalidos`.»

## Escena 6 — Replay e idempotencia (45 s)
**EN PANTALLA:** volver a correr `docker compose run --rm pipeline` y luego el consumidor.
**DECIR:** «Como Kafka guarda los eventos, puedo volver a procesar todo. Ahora hay 42 mensajes de resultados, el doble, pero siguen siendo 21 filas, porque cada resultado tiene una clave fija y el consumidor reemplaza en lugar de sumar. Eso es una salida idempotente.»

## Escena 7 — Pruebas y límites (45 s)
**EN PANTALLA:** `docker compose run --rm --no-deps app pytest -v` con las 6 pruebas en verde.
**DECIR:** «Las pruebas cubren la validación, la agregación, los duplicados y, con TestStream, un evento tardío que corrige el resultado y otro demasiado tardío que se descarta. Un límite honesto: la lectura continua de Kafka quedaba trabada en este Flink local, así que el pipeline lee con un tiempo máximo, como el smoke test de la clase 7. Por eso, en la demo, el watermark no avanza mientras lee y el descarte por lateness lo demuestro con TestStream.»

## Cierre (15 s)
**DECIR:** «En resumen: Kafka guarda los eventos, Beam usa el tiempo de evento, deduplica y agrega, y la salida es idempotente. Gracias.»
