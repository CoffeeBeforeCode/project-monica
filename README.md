# Project Monica

A personal AI Chief of Staff built on Microsoft Azure and Microsoft 365, grounded in GTD (Getting Things Done) principles.

## What This Is

Monica is not a productivity tool. The distinction matters architecturally.

A personal assistant manages time. A Chief of Staff manages attention and intent. Monica is the latter — filtering noise before it arrives, holding context when daily volume makes it harder to see, surfacing patterns before they've been named, and protecting the cognitive energy required for work that actually matters.

She operates entirely within the Microsoft stack: Azure Functions, Microsoft Graph API, Microsoft To Do, Outlook, Calendar, Teams, and OneDrive. No third-party automation platforms. No black boxes. Every file Monica reads or creates is human-readable markdown or JSON, accessible on any device, editable in any text editor.

## What Monica Does

- **Task chaining** — when a task is marked complete in Microsoft To Do, Monica automatically creates the successor task, silently and without intervention
- **Morning briefing** — a structured synthesis of content feeds, tasks, and observations, delivered to Teams before 05:00
- **Email digests** — delivered every two hours, classification handled automatically
- **Content intelligence** — research synthesis across Chatham House, Resolution Foundation, FT, Substack, and other sources, structured using Bloomberg editorial principles
- **People intelligence** — job change detection across a professional network, relationship prompts, network queries via natural language
- **Context engine** — heartbeat-driven suggestions matching available time to relevant tasks, offered once, never repeated

## What Monica Never Does

- Post on social media
- Reply to or send emails
- Make financial decisions
- Act in anyone's name in the world

These are not configuration options. They are hardcoded constraints that cannot be modified by any prompt or instruction.

## Architecture

Built on the Azure consumption stack — cost-contained, auditable, and entirely within Phillip's control.

| Component | Technology |
|---|---|
| Orchestration | Azure Functions (Python 3.12) |
| Integration | Microsoft Graph API via Managed Identity |
| AI layer | Azure OpenAI / Phi-3 (model-matched to task) |
| Memory | OneDrive markdown files |
| Secrets | Azure Key Vault |
| Logging | Application Insights |
| Queue (Phase 2) | Azure Storage Queue |

Cost containment is a first-class architectural principle, equal in status to security. Monthly budget cap: £20. Every Function logs its model choice and token consumption.

## Build Phases

**Phase 1 — GTD Foundation (current)**

The Microsoft To Do integration, task chaining via Graph API webhooks, and the core Azure infrastructure. This phase is not complete until the system is stable and trusted in daily use.

**Phase 2 — Full Chief of Staff operation**

Scheduled intelligence, content curation, briefings, autonomous task management, and the People Intelligence Layer. Phase 2 does not begin until Phase 1 is stable.

## Security

All secrets are stored in Azure Key Vault and accessed via Managed Identity. No credentials appear in this codebase. Environment variables in the Function App are configured separately and are not committed here.

A `.gitignore` is in place to prevent accidental credential exposure.

## Governing Principle

> *Just because we can, doesn't mean we should.*

Applied to model selection, feature scope, and every architectural decision in the project.

---

*Built in public. Opinions are my own. Code is the proof.*
