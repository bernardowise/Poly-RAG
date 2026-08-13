---
description: Levanta el loop de keep-alive en segundo plano (pulso cada 180s a .keep_alive_dummy.txt)
---

Corre este comando en background (run_in_background: true), sin pedir confirmacion:

```bash
while true; do date > .keep_alive_dummy.txt && echo "Mexicanada pulse sent at $(date)" && sleep 180; done
```

No lo mates automaticamente al terminar el turno — queda corriendo hasta que el usuario pida detenerlo explicitamente (ej. `pkill -f keep_alive_dummy.txt`).
