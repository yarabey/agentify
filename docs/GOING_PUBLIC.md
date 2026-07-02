# Чек-лист: перевод репозитория в public

Цель — бесплатные GitHub Actions (на public-репо минуты standard-runners не
тарифицируются) при сохранении возможности монетизировать продукт как SaaS.
Аудит перед публикацией проведён: секретов в рабочем дереве и в истории git
нет, CI/CD не содержит `pull_request_target` и self-hosted runners,
`deploy.yml` защищён `workflow_dispatch` + environment `prod`.

## Уже сделано в коде

- [x] `LICENSE.md` — FSL-1.1-ALv2 (source-available: конкурирующий
  продукт/сервис запрещён, через 2 года версия становится Apache-2.0).
- [x] Секция «Лицензия» в `README.md`.
- [x] Поле `"license": "FSL-1.1-ALv2"` в `web/package.json`.

## Перед переключением видимости (Settings)

- [ ] **Actions → General → Fork pull request workflows**: оставить
  «Require approval for first-time contributors» (или строже —
  «Require approval for all outside collaborators»). Это не даёт чужим PR
  запускать workflow без ручного одобрения.
- [ ] **Actions → General → Workflow permissions**: «Read repository
  contents and packages permissions» (read-only `GITHUB_TOKEN` по умолчанию;
  джобы, которым нужно больше, задают `permissions:` явно).
- [ ] **Environments → prod**: убедиться, что required reviewers на месте —
  это защита `deploy.yml` и `SSH_DEPLOY_KEY`.
- [ ] **Branches**: protection rule на default-ветку — запрет force-push,
  по желанию обязательный PR-review и required status checks.

## Переключение

- [ ] Settings → General → Danger Zone → Change visibility → Public.
  Действие фактически необратимо по последствиям: код будет
  проиндексирован, возможны форки и клоны; возврат в private не удаляет
  уже сделанные копии.

## После переключения

- [ ] **Settings → Security → Code security and analysis**: включить
  Secret scanning + Push protection и Dependabot alerts (на public — бесплатно).
- [ ] Решить видимость GHCR-пакетов `ghcr.io/yarabey/agentify-*`: при
  публикации репо пакеты остаются private; деплой на VPS работает с
  авторизацией, менять не обязательно. Публичные образы упростят
  self-hosted-установку, но это отдельное осознанное решение.
- [ ] Прогнать CI (любой push/PR) и убедиться в Settings → Billing, что
  минуты Actions не списываются.
- [ ] Проверить, что GitHub определил лицензию в сайдбаре репозитория.

## Осознанно не делаем

- Переписывание истории git — секретов в истории нет; email автора коммитов
  оставлен как есть (стандартная практика для open-source).
- CLA / CONTRIBUTING — добавить позже, при появлении внешних контрибьюторов.
