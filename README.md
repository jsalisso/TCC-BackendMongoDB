# 🚀 AirGuard Backend (Release 1)

Backend responsável por ingestão, processamento e disponibilização de dados do sistema **AirGuard**.

## 🧠 Arquitetura

```
ESP32 → MQTT (HiveMQ) → Backend (Render) → MongoDB Atlas → API REST
```

---

## 🔌 Health Check

### `GET /health`

Verifica o estado geral do sistema.

#### ✅ Resposta

```json
{
  "ok": true,
  "mongoConnected": true,
  "mqttConnected": true,
  "lastMessageAt": "2026-04-19T22:02:07.407Z",
  "lastTopic": "v1/CLI-001/MATRIZ/COZINHA/SN-CO-001",
  "messagesReceived": 1,
  "messagesSaved": 1,
  "messagesRejected": 0
}
```

---

## 📡 Sensores

### `GET /api/sensors`

Lista todos os sensores conhecidos no sistema.

#### 🔎 Retorno

```json
{
  "count": 1,
  "items": [
    {
      "sensor_id": "SN-CO-001",
      "cliente": "CLI-001",
      "unidade": "MATRIZ",
      "ambiente": "COZINHA",
      "profile": "COZINHA",
      "status": "LIMPO",
      "last_seen": "2026-04-19T22:02:07.407Z"
    }
  ]
}
```

---

## 📊 Overview (Dashboard)

### `GET /api/overview`

Retorna visão consolidada dos sensores (base principal do dashboard).

#### 🔎 Parâmetros opcionais

* `cliente`
* `sensor_id`
* `ambiente`
* `unidade`
* `status`
* `minutes` (janela de tempo)

#### 📌 Exemplo

```
/api/overview?cliente=CLI-001
/api/overview?minutes=60
```

---

## 📍 Última leitura

### `GET /api/latest/:sensorId`

Retorna a última leitura registrada de um sensor.

#### 📌 Exemplo

```
/api/latest/SN-CO-001
```

---

## 📚 Histórico

### `GET /api/history/:sensorId`

Retorna histórico de leituras do sensor.

#### 🔎 Parâmetros

* `limit` (default: 100, max: 1000)

#### 📌 Exemplo

```
/api/history/SN-CO-001?limit=50
```

---

## 🚨 Eventos

### `GET /api/events/:sensorId`

Lista eventos de mudança de estado (ex: LIMPO → ALERTA).

#### 🔎 Parâmetros

* `limit` (default: 100)

#### 📌 Exemplo

```
/api/events/SN-CO-001?limit=20
```

---

## 📈 Estatísticas

### `GET /api/stats/:sensorId`

Calcula métricas agregadas dentro de um intervalo de tempo.

#### 🔎 Parâmetros

* `minutes` (default: 60)

#### 📌 Exemplo

```
/api/stats/SN-CO-001?minutes=120
```

#### 📊 Métricas retornadas

* Média, mínimo e máximo:

  * temperatura
  * umidade
  * CO (ppm)
  * gás (ppm)
* Total de leituras
* Primeiro e último timestamp
* Contagem por status (LIMPO / ALERTA / PERIGO)

---

## ⚙️ Comandos (futuro)

### `POST /api/command`

Ainda não implementado.

Planejado para envio de comandos via MQTT (ex: configuração remota de sensores).

---

## 🔐 Segurança

Opcionalmente, pode ser protegido com API Key:

Header:

```
x-api-key: SUA_CHAVE
```

---

## 🌱 Variáveis de Ambiente

```
PORT=3000
MONGODB_URI=...
MONGODB_DB=TCC

MQTT_URL=mqtts://...
MQTT_USERNAME=...
MQTT_PASSWORD=...
MQTT_TOPIC_FILTER=v1/+/+/+/+

API_KEY=
```

---

## 📌 Observações

* O backend realiza ingestão contínua via MQTT
* Dados são persistidos no MongoDB Atlas
* Eventos são gerados automaticamente quando há mudança de status
* API pronta para consumo por dashboards e aplicações externas


---

## 💡 Status do Projeto

✅ Backend operacional
✅ Integração MQTT ativa
✅ Persistência em MongoDB
🚧 Dashboard em desenvolvimento

```
```
