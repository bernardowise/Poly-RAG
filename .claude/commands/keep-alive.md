---
description: Imprime el comando de keep-alive para que el usuario lo pegue en su propia terminal -- no lo ejecutes tu
---

No ejecutes nada. Solo escribe el siguiente bloque de codigo bash, tal cual, para que el
usuario lo copie y pegue en su propia terminal:

```bash
START=$(date +%s); while true; do ELAPSED=$(($(date +%s) - START)); printf "%02d:%02d:%02d\n" $((ELAPSED/3600)) $((ELAPSED%3600/60)) $((ELAPSED%60)) >> keep-alive.txt; sleep 180; done
```

No agregues explicacion, resumen, ni nada mas -- solo el bloque de codigo.
