---
description: Cambia de rama git en este mismo workspace, verificando primero que no haya cambios sin commitear
---

El usuario paso un nombre de rama como argumento (ej. /checkout paper-dev).

1. Corre `git status --short` primero. Si hay cambios sin commitear (tracked o
   untracked), muestralos y pregunta antes de continuar — no los pises.
2. Corre `git fetch origin <rama>` para asegurar que la referencia remota este
   actualizada.
3. Corre `git checkout <rama>` (usa `git checkout -b <rama> origin/<rama>` si
   la rama no existe localmente todavia).
4. Confirma con `git branch --show-current` y reporta en una linea a que rama
   se movio el workspace.
5. No hagas merge, rebase, ni ningun otro cambio — solo el checkout.
