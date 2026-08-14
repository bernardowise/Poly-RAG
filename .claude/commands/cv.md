---
description: Agrega una skill nueva y real (no solo mencionada, hecha con las manos) a .claude/claude_docs/new_skills.md
---

Revisa el trabajo reciente de esta sesion (el ultimo tool/tecnica/servicio que se
uso de forma practica -- no solo se menciono conceptualmente) y agregalo como una
entrada nueva bajo la categoria correspondiente (Claude Code, AWS, Terraform, u otra
si aplica) en .claude/claude_docs/new_skills.md.

Formato de la entrada (bullet corto, primera persona implicita, listo para CV):

```markdown
- <YYYY-MM-DD> <accion concreta hecha> -- <contexto breve de una linea>
```

Ejemplo: `- 2026-08-14 Configure IAM execution roles with least-privilege policies for Lambda -- scoped Bedrock InvokeModel to a single model ARN instead of AdministratorAccess`

Reglas:
- Solo cosas hechas realmente en esta sesion (deploy, config, debug, decision tecnica
  ejecutada) -- no listar herramientas que solo se mencionaron o discutieron sin uso real.
- No dupliques una skill ya listada -- si ya existe una entrada de la misma categoria/
  herramienta, actualiza o extiende esa en vez de crear una nueva casi identica.
- Si no hay nada nuevo y real que valga la pena archivar, dile al usuario que no
  encontraste nada en vez de inventar contenido.
- No reescribas ni borres entradas anteriores.

Este comando tambien se dispara proactivamente (sin que el usuario tenga que
invocarlo): cada vez que en la sesion se use una herramienta/tecnica/servicio nuevo
de forma practica por primera vez (ej. primer uso de terraform import, primera Lambda
desplegada, primer IAM policy least-privilege), agregalo a new_skills.md sin esperar
a que el usuario escriba /cv -- y avisale brevemente que lo hiciste.
