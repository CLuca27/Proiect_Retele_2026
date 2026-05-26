# Coada distribuita de mesaje

Proiectul implementeaza o coada distribuita de mesaje in care fiecare nod ruleaza simultan ca server si client.

Fiecare nod:
- asculta conexiuni TCP;
- incearca sa se conecteze la primul upstream disponibil;
- anunta callback host/port prin `HELLO`;
- propaga noduri noi prin `PEER_JOIN`;
- propaga `SUBSCRIBE` / `UNSUBSCRIBE`;
- accepta mesaje `PUBLISH` cu `key + payload`;
- pune mesajele intr-o coada FIFO locala;
- livreaza mesajele prin `DELIVER` catre toti consumatorii abonati;
- proceseaza mesajele si raspunde cu `ACK`.

## Structura

```text
app/
  main.py       # porneste un nod
  node.py       # server, client, broker, queue, delivery
  protocol.py   # send/recv JSON peste socket
  commands.py   # procesare pe chei
  cli.py        # trimite comenzi catre un nod pornit
Dockerfile
docker-compose.yml
README.md
```

## Protocol

Tipuri principale de mesaje:

```text
HELLO         handshake + callback host/port
HELLO_ACK     raspuns handshake
PEER_JOIN     propagare nod nou
PEER_LEFT     propagare nod indisponibil
SUBSCRIBE     abonare la o cheie
UNSUBSCRIBE   dezabonare de la o cheie
PUBLISH       producere mesaj key + payload
PUBLISH_ACK   confirmare acceptare mesaj
DELIVER       livrare catre consumer
ACK           confirmare procesare consumer
LOCAL_COMMAND comenzi locale pentru demo: subs, peers, queue
```

Payload-ul este tratat ca bytes in aplicatie si este serializat Base64 cand este transportat prin JSON.

## Comenzi implementate

Cheile procesate sunt:

```text
uppercase -> transforma textul in litere mari
reverse   -> inverseaza textul
count     -> numara caracterele
```

Exemple:

```text
uppercase + salut -> SALUT
reverse + salut   -> tulas
count + salut     -> 5
```

## Rulare cu Docker Compose

Pornire:

```bash
docker compose up --build
```

Urmarire loguri:

```bash
docker compose logs -f node1 node2 node3
```

Oprire:

```bash
docker compose down
```

Nota despre porturi Docker:

```text
5001:5000 inseamna port 5001 pe calculatorul local -> port 5000 in container.
```

Daca portul local `5001`, `5002` sau `5003` este ocupat, se poate schimba doar partea din stanga:

```yaml
ports:
  - "5101:5000"
```

Comunicarea dintre containere foloseste in continuare `node1:5000`, `node2:5000`, `node3:5000`.

## Comenzi prin CLI

Comenzile se trimit din alt terminal, in acelasi folder cu `docker-compose.yml`.

Abonare:

```bash
docker compose exec node3 python -m app.cli subscribe uppercase
```

Afisare subscrieri pe node1:

```bash
docker compose exec node1 python -m app.cli subs
```

Publicare mesaj:

```bash
docker compose exec node1 python -m app.cli publish uppercase salut
```

Alte exemple:

```bash
docker compose exec node3 python -m app.cli subscribe reverse
docker compose exec node1 python -m app.cli publish reverse salut

docker compose exec node3 python -m app.cli subscribe count
docker compose exec node1 python -m app.cli publish count "Ana are mere"
```

Dezabonare:

```bash
docker compose exec node3 python -m app.cli unsubscribe uppercase
```

Verificare noduri cunoscute:

```bash
docker compose exec node1 python -m app.cli peers
```

Verificare dimensiune coada:

```bash
docker compose exec node1 python -m app.cli queue
```

## Scenariu demo video

1. Pornire 3 noduri:

```bash
docker compose up --build
```

2. Verificare conectare in lant in loguri:

```text
node2 -> node1
node3 -> node2
node1 afla despre node3 prin PEER_JOIN
```

3. Subscribe si propagare:

```bash
docker compose exec node3 python -m app.cli subscribe uppercase
docker compose exec node1 python -m app.cli subs
```

4. Trimitere mesaj binar cu cheia `uppercase`:

```bash
docker compose exec node1 python -m app.cli publish uppercase salut
```

In loguri trebuie sa apara:

```text
node1: Queued message ...
node1: Dispatching message ...
node3: Processing message ... key=uppercase
node3: Processed message ... result=SALUT
node1: Delivered ... response={'type': 'ACK', 'status': 'OK', ...}
```

5. Demonstrare procesare diferita:

```bash
docker compose exec node3 python -m app.cli subscribe reverse
docker compose exec node1 python -m app.cli publish reverse abcdef
```

Rezultat asteptat:

```text
result=fedcba
```

6. Dezabonare:

```bash
docker compose exec node3 python -m app.cli unsubscribe uppercase
docker compose exec node1 python -m app.cli publish uppercase test
```

Dupa unsubscribe, mesajul nu mai trebuie livrat catre node3.

7. Deconectare consumer si cleanup:

```bash
docker compose exec node3 python -m app.cli subscribe uppercase
docker compose stop node3
docker compose exec node1 python -m app.cli publish uppercase salut
```

In loguri trebuie sa se vada:

```text
Delivery failed ...
Removed node node3
PEER_LEFT propagat
```

Sistemul nu trebuie sa se blocheze.

## Rulare fara Docker

Terminal 1:

```bash
python -m app.main --node-id node1 --port 5001 --callback-port 5001
```

Terminal 2:

```bash
python -m app.main --node-id node2 --port 5002 --callback-port 5002 --upstreams 127.0.0.1:5001
```

Terminal 3:

```bash
python -m app.main --node-id node3 --port 5003 --callback-port 5003 --upstreams 127.0.0.1:5002,127.0.0.1:5001
```

Comenzi locale fara Docker:

```bash
python -m app.cli --port 5003 subscribe uppercase
python -m app.cli --port 5001 publish uppercase salut
python -m app.cli --port 5001 subs
```

Daca portul cerut este ocupat, nodul incearca automat urmatoarele porturi libere.
Implicit cauta in urmatoarele 20 de porturi.

Exemplu:

```bash
python -m app.main --node-id node1 --port 5001 --port-search-limit 20
```

Daca `5001` este ocupat, nodul va incerca `5002`, `5003` etc. In loguri se afiseaza portul ales.
Pentru comenzile `cli.py`, trebuie folosit portul real afisat in loguri:

```bash
python -m app.cli --port <port-afisat> peers
```

## Upload pe server prin PuTTY/SSH

PuTTY este clientul SSH. Pentru upload se foloseste de obicei `scp`, `pscp` sau Git.

### Varianta 1: upload cu scp din Git Bash

Din folderul parinte:

```bash
cd /a/Proiecte
scp -r Proiect_Retele_2026 username@sys.ase.ro:~/
```

Pe server:

```bash
ssh username@sys.ase.ro
cd ~/Proiect_Retele_2026
```

Daca serverul are Docker:

```bash
docker compose up --build
```

### Varianta 2: upload cu pscp

Daca folositi PuTTY/PSCP pe Windows:

```bash
pscp -r A:\Proiecte\Proiect_Retele_2026 username@sys.ase.ro:/home/username/
```

### Varianta 3: prin Git

Local:

```bash
git add .
git commit -m "Add distributed message queue project"
git push origin main
```

Pe server:

```bash
git clone <repo-url>
cd Proiect_Retele_2026
```

## Note tehnice

- Livrarea este best-effort.
- Nu se garanteaza exact-once.
- Payload maxim local: 1 MB.
- Mesaj maxim in protocol: 2 MB.
- Conexiunile socket folosesc timeout.
- La esec de livrare, nodul indisponibil este eliminat din `known_nodes` si din `subscriptions`, apoi se propaga `PEER_LEFT`.
