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
  MQTT_LATENCY_TOPIC_FILTER = "v1/+/+/+/+/latency",
  API_KEY = ""
} = process.env;

if (!MONGODB_URI) throw new Error("MONGODB_URI não definido.");
if (!MQTT_URL || !MQTT_USERNAME || !MQTT_PASSWORD) {
  throw new Error("MQTT_URL, MQTT_USERNAME e MQTT_PASSWORD são obrigatórios.");
}

const app = express();

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
let latencyCollection;
let mqttClient = null;

const runtime = {
  mqttConnected: false,
  mongoConnected: false,
  lastMessageAt: null,
  lastTopic: null,
  messagesReceived: 0,
  messagesSaved: 0,
  messagesRejected: 0,
  latencyReceived: 0,
  latencySaved: 0,
  latencyRejected: 0
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

function parseLatencyTopic(topic) {
  const parts = topic.split("/");

  if (parts.length !== 6 || parts[0] !== "v1" || parts[5] !== "latency") {
    return null;
  }

  return {
    version: parts[0],
    cliente: parts[1],
    unidade: parts[2],
    ambiente: parts[3],
    sensor_id: parts[4],
    suffix: parts[5]
  };
}

function toNumberOrNull(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

async function calculateStats(filter) {
  const statsPipeline = [
    { $match: filter },
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

  const result = await telemetryCollection.aggregate(statsPipeline).toArray();
  return result[0] || null;
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

function normalizeLatencyPayload(topic, payload) {
  const topicInfo = parseLatencyTopic(topic);

  if (!topicInfo) {
    throw new Error(`Tópico de latência inválido: ${topic}`);
  }

  const receivedAt = new Date();
  const tsDeviceRaw = toNumberOrNull(payload.ts_device);
  const tsDevice = tsDeviceRaw ? new Date(tsDeviceRaw) : null;

  let latencyMs = null;

  if (tsDevice && !isNaN(tsDevice.getTime())) {
    latencyMs = receivedAt.getTime() - tsDevice.getTime();
  }

  const doc = {
    topic,
    version: topicInfo.version,
    cliente: payload.cliente ?? topicInfo.cliente,
    unidade: payload.unidade ?? topicInfo.unidade,
    ambiente: payload.ambiente ?? topicInfo.ambiente,
    sensor_id: payload.sensor_id ?? topicInfo.sensor_id,
    seq: toNumberOrNull(payload.seq),
    event: payload.event ?? null,
    ts_device_raw: tsDeviceRaw,
    ts_device: tsDevice,
    received_at: receivedAt,
    latency_ms: latencyMs,
    raw_payload: payload
  };

  if (!doc.sensor_id) {
    throw new Error("sensor_id ausente no payload de latência.");
  }

  return doc;
}

async function connectMongo() {
  await mongoClient.connect();

  db = mongoClient.db(MONGODB_DB);
  telemetryCollection = db.collection("telemetria");
  eventsCollection = db.collection("eventos");
  latencyCollection = db.collection("latencia");

  await telemetryCollection.createIndex({ sensor_id: 1, received_at: -1 });
  await telemetryCollection.createIndex({ cliente: 1, received_at: -1 });
  await telemetryCollection.createIndex({ cliente: 1, unidade: 1, ambiente: 1, received_at: -1 });
  await telemetryCollection.createIndex({ status: 1, received_at: -1 });

  await eventsCollection.createIndex({ sensor_id: 1, created_at: -1 });
  await eventsCollection.createIndex({ cliente: 1, created_at: -1 });

  await latencyCollection.createIndex({ sensor_id: 1, received_at: -1 });
  await latencyCollection.createIndex({ cliente: 1, received_at: -1 });
  await latencyCollection.createIndex({ seq: 1, sensor_id: 1 });

  runtime.mongoConnected = true;
  console.log("MongoDB conectado.");
}

async function handleTelemetryMessage(topic, parsed) {
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
    console.error("Erro ao salvar telemetria no MongoDB:", err.message);
  }
}

async function handleLatencyMessage(topic, parsed) {
  runtime.latencyReceived += 1;

  let doc;

  try {
    doc = normalizeLatencyPayload(topic, parsed);
  } catch (err) {
    runtime.latencyRejected += 1;
    console.error("Payload de latência rejeitado:", err.message);
    return;
  }

  try {
    await latencyCollection.insertOne(doc);
    runtime.latencySaved += 1;

    console.log(
      `[LATENCY ${doc.sensor_id}] seq=${doc.seq} latency_ms=${doc.latency_ms}`
    );
  } catch (err) {
    console.error("Erro ao salvar latência no MongoDB:", err.message);
  }
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
        console.error("Erro ao assinar tópico de telemetria:", err.message);
        return;
      }
      console.log(`Assinado em: ${MQTT_TOPIC_FILTER}`);
    });

    mqttClient.subscribe(MQTT_LATENCY_TOPIC_FILTER, { qos: 0 }, (err) => {
      if (err) {
        console.error("Erro ao assinar tópico de latência:", err.message);
        return;
      }
      console.log(`Assinado em: ${MQTT_LATENCY_TOPIC_FILTER}`);
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

    if (topic.endsWith("/latency")) {
      await handleLatencyMessage(topic, parsed);
      return;
    }

    await handleTelemetryMessage(topic, parsed);
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

function buildRangeFilter(query = {}, dateField = "received_at") {
  const range = {};

  if (query.start) {
    const startDate = new Date(query.start);
    if (isNaN(startDate.getTime())) {
      throw new Error("Parâmetro start inválido.");
    }
    range.$gte = startDate;
  }

  if (query.end) {
    const endDate = new Date(query.end);
    if (isNaN(endDate.getTime())) {
      throw new Error("Parâmetro end inválido.");
    }
    range.$lte = endDate;
  }

  if (Object.keys(range).length === 0) {
    return {};
  }

  return { [dateField]: range };
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
    messagesRejected: runtime.messagesRejected,
    latencyReceived: runtime.latencyReceived,
    latencySaved: runtime.latencySaved,
    latencyRejected: runtime.latencyRejected
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

    const limit = Math.min(Number(req.query.limit) || 50, 1000);
    const page = Math.max(Number(req.query.page) || 1, 1);
    const skip = (page - 1) * limit;

    const filter = { sensor_id: sensorId };

    try {
      Object.assign(filter, buildRangeFilter(req.query, "received_at"));
    } catch (err) {
      return res.status(400).json({ error: err.message });
    }

    const [total, docs] = await Promise.all([
      telemetryCollection.countDocuments(filter),
      telemetryCollection
        .find(filter)
        .sort({ received_at: -1 })
        .skip(skip)
        .limit(limit)
        .toArray()
    ]);

    const totalPages = Math.ceil(total / limit);

    res.json({
      sensor_id: sensorId,
      count: docs.length,
      total,
      page,
      limit,
      totalPages,
      start: req.query.start || null,
      end: req.query.end || null,
      items: docs
    });
  } catch (err) {
    res.status(500).json({ error: "Erro ao buscar histórico.", detail: err.message });
  }
});

app.get("/api/events/:sensorId", async (req, res) => {
  try {
    const { sensorId } = req.params;
    const limit = Math.min(Number(req.query.limit) || 100, 500);

    const filter = { sensor_id: sensorId };

    try {
      Object.assign(filter, buildRangeFilter(req.query, "created_at"));
    } catch (err) {
      return res.status(400).json({ error: err.message });
    }

    const docs = await eventsCollection
      .find(filter)
      .sort({ created_at: -1 })
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
    res.status(500).json({ error: "Erro ao buscar eventos.", detail: err.message });
  }
});

app.get("/api/stats-range/:sensorId", async (req, res) => {
  try {
    const { sensorId } = req.params;

    const filter = { sensor_id: sensorId };

    try {
      Object.assign(filter, buildRangeFilter(req.query, "received_at"));
    } catch (err) {
      return res.status(400).json({ error: err.message });
    }

    const stats = await calculateStats(filter);

    res.json({
      sensor_id: sensorId,
      start: req.query.start || null,
      end: req.query.end || null,
      stats
    });
  } catch (err) {
    res.status(500).json({
      error: "Erro ao calcular estatísticas por período.",
      detail: err.message
    });
  }
});

app.get("/api/stats/:sensorId", async (req, res) => {
  try {
    const { sensorId } = req.params;
    const minutes = Math.min(Number(req.query.minutes) || 60, 7 * 24 * 60);
    const since = new Date(Date.now() - minutes * 60 * 1000);

    const filter = {
      sensor_id: sensorId,
      received_at: { $gte: since }
    };

    const stats = await calculateStats(filter);

    res.json({
      sensor_id: sensorId,
      since,
      stats
    });
  } catch (err) {
    res.status(500).json({
      error: "Erro ao calcular estatísticas.",
      detail: err.message
    });
  }
});

app.get("/api/latency/:sensorId", async (req, res) => {
  try {
    const { sensorId } = req.params;

    const filter = {
      sensor_id: sensorId,
      latency_ms: { $ne: null }
    };

    try {
      Object.assign(filter, buildRangeFilter(req.query, "received_at"));
    } catch (err) {
      return res.status(400).json({ error: err.message });
    }

    const pipeline = [
      { $match: filter },
      { $sort: { received_at: 1 } },
      {
        $group: {
          _id: "$sensor_id",
          total: { $sum: 1 },
          avg_latency_ms: { $avg: "$latency_ms" },
          min_latency_ms: { $min: "$latency_ms" },
          max_latency_ms: { $max: "$latency_ms" },
          first_seen: { $first: "$received_at" },
          last_seen: { $last: "$received_at" }
        }
      }
    ];

    const [summary] = await latencyCollection.aggregate(pipeline).toArray();

    const latest = await latencyCollection
      .find(filter)
      .sort({ received_at: -1 })
      .limit(20)
      .toArray();

    res.json({
      sensor_id: sensorId,
      start: req.query.start || null,
      end: req.query.end || null,
      summary: summary || null,
      latest
    });
  } catch (err) {
    res.status(500).json({
      error: "Erro ao calcular latência.",
      detail: err.message
    });
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