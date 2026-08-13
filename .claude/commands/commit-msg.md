---
description: Redacta un mensaje de commit siguiendo las reglas del proyecto — sin comillas, formato type(scope), limite 500 caracteres, nunca ejecuta git commit
---

Revisa git status y git diff de los cambios staged/unstaged actuales.
Redacta un mensaje de commit que siga estrictamente:
- Formato: type(scope): short summary, linea en blanco, bullets con -
- CERO comillas de cualquier tipo (ni simples, ni dobles, ni tipograficas, ni backticks), sin emoji, solo ASCII plano
- Limite duro de 500 caracteres totales (asunto + cuerpo)
- NUNCA ejecutes git commit — solo entrega el texto del mensaje y detente
