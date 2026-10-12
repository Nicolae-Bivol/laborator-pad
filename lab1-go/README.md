# Aplicația de mesagerie — ghid de utilizare

Aplicația permite trimiterea mesajelor și anunțurilor către destinatari grupați pe canale. Se folosește din terminal și are trei roluri:

- **Broker**: primește mesajele și le distribuie către receiverii abonați la canalul respectiv.
- **Receiver**: primește și salvează mesajele unui singur canal.
- **Sender**: trimite un mesaj, apoi se închide.

Un mesaj ajunge la fiecare receiver abonat la tipul său. De exemplu, un mesaj `info` ajunge la toți receiverii pentru `info`, iar o alertă `alert` ajunge la toți receiverii pentru `alert`.

## 1. Pregătirea terminalelor

Ai nevoie de Go instalat. Verifică într-un terminal:

```powershell
go version
```

În VS Code, deschide terminale separate folosind butonul **+**. În fiecare terminal, intră în folderul proiectului înainte de a rula comenzile:

```powershell
cd "C:\Users\bvlni\OneDrive\Desktop\Laborator PAD\lab1-go"
```

Dacă proiectul este în altă locație, adaptează calea. Terminalul trebuie să fie în folderul care conține `go.mod`.

## 2. Pornirea brokerului, receiverilor și senderului

### Terminalul 1 — broker

```powershell
go run . broker
```

Lasă acest terminal deschis. Brokerul folosește implicit adresa `127.0.0.1:7070` și canalele `info` și `alert`.

### Terminalul 2 — receiver pentru mesaje obișnuite

```powershell
go run . receiver -client r1 -type info -inbox data/r1.jsonl
```

Receiverul `r1` primește mesaje de tip `info`. Lasă-l pornit.

### Terminalul 3 — receiver pentru alerte

```powershell
go run . receiver -client r2 -type alert -inbox data/r2.jsonl
```

Receiverul `r2` primește mesaje de tip `alert`. Lasă-l pornit.

### Terminalul 4 — trimiterea mesajelor

Trimite un mesaj obișnuit:

```powershell
go run . sender -id mesaj-001 -type info -text "Salut!"
```

Îl primește `r1`. Receiverul `r2` nu îl primește, deoarece este abonat la alt canal.

Trimite o alertă din același terminal:

```powershell
go run . sender -id alerta-001 -type alert -text "Atenție: serverul va fi oprit la ora 18:00."
```

O primește `r2`. Dacă există mai mulți receiveri abonați la `alert`, fiecare primește o copie.

Senderul se închide după fiecare trimitere. Poți executa următoarea comandă în același terminal.

### Ce apare în terminale

| Mesaj afișat | Ce înseamnă |
|---|---|
| `Broker TCP ...` | Brokerul este pornit și așteaptă conexiuni. |
| `Abonat client=... tip=...` | Receiverul s-a conectat la canal. |
| `ACCEPTAT de broker ...` | Brokerul a acceptat mesajul; receiverul poate să îl proceseze ulterior. |
| `PRIMIT ... text=...` | Receiverul a salvat mesajul primit. |
| `ACK client=... id=...` | Brokerul a primit confirmarea receiverului. |
| `DUPLICAT IGNORAT ...` | Receiverul avea deja mesajul salvat și nu îl salvează din nou. |

## 3. Trimiterea unui anunț către mai mulți destinatari

Pentru un anunț către toți abonații la `info`, păstrează receiverul `r1` pornit și deschide încă un terminal pentru `r3`:

```powershell
go run . receiver -client r3 -type info -inbox data/r3.jsonl
```

În terminalul senderului, trimite:

```powershell
go run . sender -id anunt-001 -type info -text "Anunț: ședința începe la ora 10:00."
```

**Atât `r1`, cât și `r3` primesc anunțul.** Receiverul `r2`, abonat la `alert`, nu îl primește.

| Tipul mesajului | Cine îl primește în această configurație |
|---|---|
| `info` | `r1` și `r3` |
| `alert` | `r2` |

Un receiver nou primește și istoricul păstrat pentru canalul său. De aceea, la pornirea lui `r3`, pot apărea mesaje `info` trimise înainte de conectarea lui.

## 4. Cum schimbi mesajele și destinatarii

### Schimbarea textului

Modifică valoarea lui `-text` și folosește un ID nou:

```powershell
go run . sender -id mesaj-002 -type info -text "Acesta este noul mesaj."
```

### Schimbarea canalului de trimitere

Modifică `-type` pentru a alege grupul de destinatari:

```powershell
go run . sender -id alerta-002 -type alert -text "Anunț urgent pentru abonații la alerte."
```

Destinatarii sunt aleși prin canal, nu prin ID-ul receiverului. Senderul nu are un argument pentru trimitere directă către `r1` sau `r2`. Pentru un singur destinatar, folosește un canal la care este abonat doar acel receiver.

### Adăugarea unui destinatar

Pornește un receiver într-un terminal separat, cu un nume și un fișier propriu:

```powershell
go run . receiver -client r4 -type info -inbox data/r4.jsonl
```

Acum și `r4` primește mesajele `info`. Fiecare receiver pornit simultan trebuie să aibă un `-client` diferit și un fișier `-inbox` diferit.

### Mutarea unui receiver pe alt canal

Oprește receiverul respectiv cu **Ctrl+C**, apoi pornește-l cu o identitate nouă și un inbox nou:

```powershell
go run . receiver -client r1-alert -type alert -inbox data/r1-alert.jsonl
```

Acest receiver va primi alerte. Identitatea veche `r1` rămâne asociată cu `info` în datele brokerului; nu o poți refolosi pentru `alert` în aceeași configurație salvată.

### Crearea unor canale proprii

Poți folosi nume precum `anunturi`, `studenti` sau `profesori`. Mai întâi oprește brokerul și receiverii cu **Ctrl+C**.

În terminalul brokerului, pornește o configurație separată:

```powershell
go run . broker -types anunturi,studenti,profesori -data data/grupuri-broker.jsonl
```

Pentru prima folosire, alege un nume de jurnal care nu a mai fost utilizat. Dacă repornești cu un jurnal existent, canalele și mesajele vechi sunt păstrate, iar canalele noi sunt adăugate.

Pornește fiecare receiver într-un terminal separat:

```powershell
go run . receiver -client student-1 -type studenti -inbox data/student-1.jsonl
```

```powershell
go run . receiver -client profesor-1 -type profesori -inbox data/profesor-1.jsonl
```

```powershell
go run . receiver -client cititor-anunturi -type anunturi -inbox data/cititor-anunturi.jsonl
```

În terminalul senderului, trimite mesajele:

```powershell
go run . sender -id studenti-001 -type studenti -text "Tema trebuie trimisă până vineri."
```

```powershell
go run . sender -id profesori-001 -type profesori -text "Ședința profesorilor este luni."
```

```powershell
go run . sender -id anunturi-001 -type anunturi -text "Biblioteca este închisă mâine."
```

| Mesaj | Cine îl primește |
|---|---|
| `studenti-001` | Toți receiverii abonați la `studenti` |
| `profesori-001` | Toți receiverii abonați la `profesori` |
| `anunturi-001` | Toți receiverii abonați la `anunturi` |

Un receiver ascultă un singur canal. Dacă aceeași persoană trebuie să primească atât mesaje pentru studenți, cât și anunțuri generale, pornește pentru ea două procese receiver, cu identități și inbox-uri diferite.

## 5. Oprirea, repornirea și păstrarea mesajelor

Oprește brokerul sau un receiver cu **Ctrl+C** în terminalul său.

Implicit, brokerul păstrează datele în `data/broker.jsonl`, iar receiverii salvează mesajele în fișierele indicate prin `-inbox`.

Pentru a continua după o oprire, pornește brokerul cu același jurnal și receiverii cu aceleași valori pentru `-client`, `-type` și `-inbox`.

Dacă un receiver este oprit, poți trimite în continuare mesaje cât timp brokerul este pornit. Receiverul le primește după reconectare. Dacă brokerul este oprit, receiverii încearcă să se reconecteze automat.

Pentru o sesiune fără păstrarea datelor brokerului pe disc, oprește brokerul existent și pornește:

```powershell
go run . broker -storage memory
```

Mesajele și abonamentele brokerului se pierd la oprirea lui. Fișierele inbox ale receiverilor rămân salvate.

Pentru o sesiune separată cu date noi, folosește un jurnal, identități și inbox-uri noi:

Terminalul brokerului:

```powershell
go run . broker -data data/sesiune-2-broker.jsonl
```

Terminalul receiverului:

```powershell
go run . receiver -client sesiune-2-r1 -type info -inbox data/sesiune-2-r1.jsonl
```

Terminalul senderului:

```powershell
go run . sender -id sesiune-2-mesaj-001 -type info -text "Primul mesaj din noua sesiune."
```

## 6. Reguli și probleme frecvente

- **Un mesaj nou are un ID nou.** Nu folosi același ID pentru texte sau tipuri diferite. Pentru retrimiterea aceluiași mesaj, păstrează ID-ul, tipul și textul.
- **`go.mod file not found`**: terminalul este în alt folder. Intră în `lab1-go` înainte de `go run .`.
- **Conexiune refuzată / `retry publicare`**: verifică dacă brokerul este pornit și dacă toate procesele folosesc aceeași adresă.
- **Port ocupat**: oprește brokerul existent înainte de a porni altul pe aceeași adresă.
- **`tip necunoscut` / `canal necunoscut`**: canalul trebuie configurat la pornirea brokerului prin `-types`.
- **`client deja conectat`**: există deja un receiver cu acel `-client`. Oprește-l sau alege altă identitate și alt inbox.
- **`clientul este legat de alt tip`**: folosește o identitate nouă și un inbox nou pentru noul canal.
- **`ID existent cu alt continut`**: ai refolosit un ID pentru un mesaj diferit. Alege un ID nou.
- **Mesaj acceptat, dar nu apare la receiver**: verifică dacă receiverul este conectat și dacă `-type` coincide cu tipul mesajului. Un mesaj deja salvat și confirmat nu este afișat din nou doar pentru că îl retrimiți.
- **`DLQ` în broker**: mesajul a epuizat încercările de livrare fără confirmare pentru acel receiver; implicit sunt 3 încercări. Aplicația nu are o comandă pentru reluarea automată a mesajelor din DLQ.

Pentru alt port, folosește aceeași valoare `-addr` la toate cele trei roluri. De exemplu, în terminale separate:

```powershell
go run . broker -addr 127.0.0.1:8080
```

```powershell
go run . receiver -addr 127.0.0.1:8080 -client r1 -type info -inbox data/r1.jsonl
```

```powershell
go run . sender -addr 127.0.0.1:8080 -id mesaj-8080-001 -type info -text "Mesaj trimis pe portul 8080."
```
