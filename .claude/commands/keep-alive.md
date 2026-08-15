---
description: Imprime el comando de keep-alive para que el usuario lo pegue en su propia terminal -- no lo ejecutes tu
---

No ejecutes nada. Solo escribe el siguiente bloque de codigo bash, tal cual, para que el
usuario lo copie y pegue en su propia terminal:

```bash
while true; do date > .keep_alive_dummy.txt && echo "🇲🇽 Mexicanada pulse sent at $(date)" && sleep 180; done
```

No agregues explicacion, resumen, ni nada mas -- solo el bloque de codigo.
