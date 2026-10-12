# Laborator PAD

Aplicație de mesagerie în Go, cu broker, sender și receiver.

Proiectul se află în folderul [`lab1-go`](lab1-go/). Comenzile de pornire, trimiterea mesajelor și configurarea destinatarilor sunt explicate în [ghidul de utilizare](lab1-go/README.md).

Din rădăcina repository-ului, intră în proiect înainte de a rula comenzile:

```powershell
cd lab1-go
```

Pornește fiecare componentă într-un terminal separat, din folderul `lab1-go`:

```powershell
go run . broker
```

```powershell
go run . receiver -client r1 -type info -inbox data/r1.jsonl
```

```powershell
go run . sender -id mesaj-001 -type info -text "Salut!"
```

Mesajele salvate local și executabilele generate sunt excluse din repository prin `.gitignore`.
