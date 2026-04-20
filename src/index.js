import dotenv from "dotenv";
import express from "express";
import cors from "cors";
import mqtt from "mqtt";
import { MongoClient } from "mongodb";

dotenv.config();

const {
  PORT = 3000,
  MONGODB_URI,
  MONGODB_DB = "TCC",
  MQTT_URL,
  MQTT_USERNAME,
  MQTT_PASSWORD,
  MQTT_TOPIC_FILTER = "v1/+/+/+/+",
  API_KEY = ""
} = process.env;

if (!MONGODB_URI) {
  throw new Error("MONGODB_URI não definido.");
}

if (!MQTT_URL || !MQTT_USERNAME || !MQTT_PASSWORD) {
  throw new Error("MQTT_URL, MQTT_USERNAME e MQTT_PASSWORD são obrigatórios.");
}

const app = express();

// CORS liberado para o dashboard consumir a API
app.use(cors());
app.use(express.json());

function requireApiKey(req, res, next) {
  if (!API_KEY) return next();

  const incoming = req.header("x-api-key");
  if (incoming !== API_KEY) {
    return res.status(401).json({ error: "Não autorizado." });
  }

  next();
}

app.use("/api", requireApiKey);

const mongoClient = new MongoClient(MONGODB_URI);

let db;
let telemetryCollection;
let eventsCollection;
let mqttClient = null;

const runtime = {
  mqttConnected: false,
  mongoConnected: false,
  lastMessageAt: null,
  lastTopic: null,
  messagesReceived: 0,
  messagesSaved: 0,
  messagesRejected: 0
};

function parseTopic(topic) {
  const parts = topic.split("/");

  if (parts.length !== 5 || parts[0] !== "v1") {
    return null;
  }

  return {
    version: parts[0],
    cliente: parts[1],
    unidade: parts[2],
    ambiente: parts[3],
    sensor_id: parts[4]
  };
}

function toNumberOrNull(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function normalizePayload(topic, payload) {
  const topicInfo = parseTopic(topic);

  if (!topicInfo) {
    throw new Error(`Tópico inválido: ${topic}`);
  }

  const doc = {
    topic,
    version: topicInfo.version,
    cliente: payload.cliente ?? topicInfo.cliente,
    unidade: payload.unidade ?? topicInfo.unidade,
    ambiente: payload.ambiente ?? topicInfo.ambiente,
    sensor_id: payload.sensor_id ?? topicInfo.sensor_id,
    profile: payload.profile ?? null,
    status: payload.status ?? null,
    presenca: Boolean(payload.presenca),
    buzzer: payload.buzzer ?? null,
    temp_c: toNumberOrNull(payload.temp_c),
    umid_pct: toNumberOrNull(payload.umid_pct),
    r0_mq7: toNumberOrNull(payload.r0_mq7),
    r0_mq4: toNumberOrNull(payload.r0_mq4),
    thresholds: {
      co_alerta: toNumberOrNull(payload?.thresholds?.co_alerta),
      co_perigo: toNumberOrNull(payload?.thresholds?.co_perigo),
      gas_alerta: toNumberOrNull(payload?.thresholds?.gas_alerta),
      gas_perigo: toNumberOrNull(payload?.thresholds?.gas_perigo)
    },
    leitura: {
      co_ppm: toNumberOrNull(payload?.leitura?.co_ppm),
      metano_ppm: toNumberOrNull(payload?.leitura?.metano_ppm),
      flame_detected: payload?.leitura?.flame_detected ?? null
    },
    raw_payload: payload,
    received_at: new Date()
  };

  if (!doc.sensor_id) {
    throw new Error("sensor_id ausente.");
  }

  return doc;
}

async function connectMongo() {
  await mongoClient.connect();

  db = mongoClient.db(MONGODB_DB);
  telemetryCollection = db.collection("telemetria");
  eventsCollection = db.collection("eventos");

  await telemetryCollection.createIndex({ sensor_id: 1, received_at: -1 });
  await telemetryCollection.createIndex({ cliente: 1, received_at: -1 });
  await telemetryCollection.createIndex({ cliente: 1, unidade: 1, ambiente: 1, received_at: -1 });
  await telemetryCollection.createIndex({ status: 1, received_at: -1 });

  await eventsCollection.createIndex({ sensor_id: 1, created_at: -1 });
  await eventsCollection.createIndex({ cliente: 1, created_at: -1 });

  runtime.mongoConnected = true;
  console.log("MongoDB conectado.");
}

function connectMqtt() {
  mqttClient = mqtt.connect(MQTT_URL, {
    username: MQTT_USERNAME,
    password: MQTT_PASSWORD,
    reconnectPeriod: 5000,
    connectTimeout: 30000,
    clean: true
  });

  mqttClient.on("connect", () => {
    runtime.mqttConnected = true;
    console.log("MQTT conectado.");

    mqttClient.subscribe(MQTT_TOPIC_FILTER, { qos: 0 }, (err) => {
      if (err) {
        console.error("Erro ao assinar tópico:", err.message);
        return;
      }
      console.log(`Assinado em: ${MQTT_TOPIC_FILTER}`);
    });
  });

  mqttClient.on("reconnect", () => {
    runtime.mqttConnected = false;
    console.log("Reconectando ao MQTT...");
  });

  mqttClient.on("close", () => {
    runtime.mqttConnected = false;
    console.log("MQTT desconectado.");
  });

  mqttClient.on("error", (err) => {
    runtime.mqttConnected = false;
    console.error("Erro MQTT:", err.message);
  });

  mqttClient.on("message", async (topic, buffer) => {
    runtime.messagesReceived += 1;
    runtime.lastMessageAt = new Date();
    runtime.lastTopic = topic;

    let parsed;
    try {
      parsed = JSON.parse(buffer.toString("utf8"));
    } catch (err) {
      runtime.messagesRejected += 1;
      console.error("Payload não é JSON válido:", err.message);
      return;
    }

    let doc;
    try {
      doc = normalizePayload(topic, parsed);
    } catch (err) {
      runtime.messagesRejected += 1;
      console.error("Mensagem rejeitada:", err.message);
      return;
    }

    try {
      const previous = await telemetryCollection.findOne(
        { sensor_id: doc.sensor_id },
        {
          sort: { received_at: -1 },
          projection: { status: 1, received_at: 1 }
        }
      );

      await telemetryCollection.insertOne(doc);
      runtime.messagesSaved += 1;

      if (previous?.status && previous.status !== doc.status) {
        await eventsCollection.insertOne({
          sensor_id: doc.sensor_id,
          cliente: doc.cliente,
          unidade: doc.unidade,
          ambiente: doc.ambiente,
          tipo: "TRANSICAO_ESTADO",
          de: previous.status,
          para: doc.status,
          created_at: new Date(),
          source_topic: topic
        });
      }

      console.log(
        `[${doc.sensor_id}] ${doc.status} | CO=${doc.leitura.co_ppm} | GAS=${doc.leitura.metano_ppm}`
      );
    } catch (err) {
      console.error("Erro ao salvar no MongoDB:", err.message);
    }
  });
}

function buildDashboardFilter(query = {}) {
  const filter = {};

  if (query.cliente) filter.cliente = query.cliente;
  if (query.sensor_id) filter.sensor_id = query.sensor_id;
  if (query.ambiente) filter.ambiente = query.ambiente;
  if (query.unidade) filter.unidade = query.unidade;
  if (query.status) filter.status = query.status;

  if (query.minutes) {
    const minutes = Math.min(Number(query.minutes) || 60, 7 * 24 * 60);
    filter.received_at = { $gte: new Date(Date.now() - minutes * 60 * 1000) };
  }

  return filter;
}

app.get("/health", async (_req, res) => {
  res.json({
    ok: runtime.mongoConnected && runtime.mqttConnected,
    mongoConnected: runtime.mongoConnected,
    mqttConnected: runtime.mqttConnected,
    lastMessageAt: runtime.lastMessageAt,
    lastTopic: runtime.lastTopic,
    messagesReceived: runtime.messagesReceived,
    messagesSaved: runtime.messagesSaved,
    messagesRejected: runtime.messagesRejected
  });
});

app.get("/api/sensors", async (_req, res) => {
  try {
    const pipeline = [
      { $sort: { received_at: -1 } },
      {
        $group: {
          _id: "$sensor_id",
          sensor_id: { $first: "$sensor_id" },
          cliente: { $first: "$cliente" },
          unidade: { $first: "$unidade" },
          ambiente: { $first: "$ambiente" },
          profile: { $first: "$profile" },
          status: { $first: "$status" },
          last_seen: { $first: "$received_at" }
        }
      },
      { $sort: { cliente: 1, unidade: 1, ambiente: 1, sensor_id: 1 } }
    ];

    const items = await telemetryCollection.aggregate(pipeline).toArray();

    res.json({
      count: items.length,
      items
    });
  } catch (err) {
    res.status(500).json({ error: "Erro ao listar sensores.", detail: err.message });
  }
});

app.get("/api/overview", async (req, res) => {
  try {
    const match = buildDashboardFilter(req.query);

    const pipeline = [
      { $match: match },
      { $sort: { received_at: -1 } },
      {
        $group: {
          _id: "$sensor_id",
          sensor_id: { $first: "$sensor_id" },
          cliente: { $first: "$cliente" },
          unidade: { $first: "$unidade" },
          ambiente: { $first: "$ambiente" },
          profile: { $first: "$profile" },
          status: { $first: "$status" },
          temp_c: { $first: "$temp_c" },
          umid_pct: { $first: "$umid_pct" },
          co_ppm: { $first: "$leitura.co_ppm" },
          metano_ppm: { $first: "$leitura.metano_ppm" },
          presenca: { $first: "$presenca" },
          last_seen: { $first: "$received_at" }
        }
      },
      { $sort: { cliente: 1, unidade: 1, ambiente: 1, sensor_id: 1 } }
    ];

    const items = await telemetryCollection.aggregate(pipeline).toArray();

    res.json({
      count: items.length,
      items
    });
  } catch (err) {
    res.status(500).json({ error: "Erro ao gerar overview.", detail: err.message });
  }
});

app.get("/api/latest/:sensorId", async (req, res) => {
  try {
    const { sensorId } = req.params;

    const doc = await telemetryCollection.findOne(
      { sensor_id: sensorId },
      { sort: { received_at: -1 } }
    );

    if (!doc) {
      return res.status(404).json({ error: "Sensor não encontrado." });
    }

    res.json(doc);
  } catch (err) {
    res.status(500).json({ error: "Erro ao buscar última leitura.", detail: err.message });
  }
});

app.get("/api/history/:sensorId", async (req, res) => {
  try {
    const { sensorId } = req.params;
    const limit = Math.min(Number(req.query.limit) || 100, 1000);

    const filter = { sensor_id: sensorId };

    if (req.query.start || req.query.end) {
      filter.received_at = {};

      if (req.query.start) {
        const startDate = new Date(req.query.start);
        if (isNaN(startDate.getTime())) {
          return res.status(400).json({ error: "Parâmetro start inválido." });
        }
        filter.received_at.$gte = startDate;
      }

      if (req.query.end) {
        const endDate = new Date(req.query.end);
        if (isNaN(endDate.getTime())) {
          return res.status(400).json({ error: "Parâmetro end inválido." });
        }
        filter.received_at.$lte = endDate;
      }
    }

    const docs = await telemetryCollection
      .find(filter)
      .sort({ received_at: -1 })
      .limit(limit)
      .toArray();

    res.json({
      sensor_id: sensorId,
      count: docs.length,
      start: req.query.start || null,
      end: req.query.end || null,
      items: docs
    });
  } catch (err) {
    res.status(500).json({ error: "Erro ao buscar histórico.", detail: err.message });
  }
});

/* app.get("/api/history/:sensorId", async (req, res) => {
  try {
    const { sensorId } = req.params;
    const limit = Math.min(Number(req.query.limit) || 100, 1000);

    const docs = await telemetryCollection
      .find({ sensor_id: sensorId })
      .sort({ received_at: -1 })
      .limit(limit)
      .toArray();

    res.json({
      sensor_id: sensorId,
      count: docs.length,
      items: docs
    });
  } catch (err) {
    res.status(500).json({ error: "Erro ao buscar histórico.", detail: err.message });
  }
}); */

app.get("/api/events/:sensorId", async (req, res) => {
  try {
    const { sensorId } = req.params;
    const limit = Math.min(Number(req.query.limit) || 100, 500);

    const docs = await eventsCollection
      .find({ sensor_id: sensorId })
      .sort({ created_at: -1 })
      .limit(limit)
      .toArray();

    res.json({
      sensor_id: sensorId,
      count: docs.length,
      items: docs
    });
  } catch (err) {
    res.status(500).json({ error: "Erro ao buscar eventos.", detail: err.message });
  }
});

app.get("/api/stats/:sensorId", async (req, res) => {
  try {
    const { sensorId } = req.params;
    const minutes = Math.min(Number(req.query.minutes) || 60, 7 * 24 * 60);
    const since = new Date(Date.now() - minutes * 60 * 1000);

    const statsPipeline = [
      {
        $match: {
          sensor_id: sensorId,
          received_at: { $gte: since }
        }
      },
      { $sort: { received_at: 1 } },
      {
        $group: {
          _id: "$sensor_id",
          total: { $sum: 1 },
          avg_temp_c: { $avg: "$temp_c" },
          min_temp_c: { $min: "$temp_c" },
          max_temp_c: { $max: "$temp_c" },
          avg_umid_pct: { $avg: "$umid_pct" },
          min_umid_pct: { $min: "$umid_pct" },
          max_umid_pct: { $max: "$umid_pct" },
          avg_co_ppm: { $avg: "$leitura.co_ppm" },
          min_co_ppm: { $min: "$leitura.co_ppm" },
          max_co_ppm: { $max: "$leitura.co_ppm" },
          avg_gas_ppm: { $avg: "$leitura.metano_ppm" },
          min_gas_ppm: { $min: "$leitura.metano_ppm" },
          max_gas_ppm: { $max: "$leitura.metano_ppm" },
          last_status: { $last: "$status" },
          first_seen: { $first: "$received_at" },
          last_seen: { $last: "$received_at" }
        }
      }
    ];

    const statusCountPipeline = [
      {
        $match: {
          sensor_id: sensorId,
          received_at: { $gte: since }
        }
      },
      {
        $group: {
          _id: "$status",
          total: { $sum: 1 }
        }
      }
    ];

    const [statsResult, statusResult] = await Promise.all([
      telemetryCollection.aggregate(statsPipeline).toArray(),
      telemetryCollection.aggregate(statusCountPipeline).toArray()
    ]);

    const statusCounts = statusResult.reduce((acc, item) => {
      acc[item._id || "DESCONHECIDO"] = item.total;
      return acc;
    }, {});

    res.json({
      sensor_id: sensorId,
      since,
      stats: statsResult[0] ?? null,
      status_counts: statusCounts
    });
  } catch (err) {
    res.status(500).json({ error: "Erro ao calcular estatísticas.", detail: err.message });
  }
});

app.post("/api/command", async (_req, res) => {
  res.status(501).json({
    error: "Ainda não implementado.",
    hint: "A próxima release pode publicar comandos MQTT no tópico /cmd."
  });
});

async function start() {
  await connectMongo();
  connectMqtt();

  app.listen(PORT, () => {
    console.log(`API ouvindo na porta ${PORT}`);
  });
}

start().catch((err) => {
  console.error("Falha ao iniciar aplicação:", err);
  process.exit(1);
});