import fs from "node:fs";
import { createGzip } from "node:zlib";
import axios from "axios";
import AdmZip from "adm-zip";

const ROOT = process.env.SOURCE_URL_ROOT; // ej: https://www.dnit.gov.py/documents/20123/662360/ruc
if (!ROOT) { console.error("Falta SOURCE_URL_ROOT"); process.exit(1); }

const outCsv = "data/padron_ruc.csv";
const outGz  = "data/padron_ruc.csv.gz";

// encabezado
fs.mkdirSync("data", { recursive: true });
fs.writeFileSync(outCsv, "ruc|razonSocial|digitoVerificador|rucAnterior|estado|fechaHoraImportacion\n");

const fecha = new Date().toISOString();

for (let d = 0; d <= 9; d++) {
  const url = `${ROOT}${d}.zip`;
  console.log("Descargando:", url);
  const res = await axios.get(url, { responseType: "arraybuffer", validateStatus: () => true });
  if (res.status !== 200) {
    console.warn("Saltando dígito", d, "HTTP", res.status);
    continue;
  }
  const zip = new AdmZip(Buffer.from(res.data));
  const entry = zip.getEntries()[0];
  if (!entry) continue;
  const lines = entry.getData().toString("utf8").split(/\r?\n/);
  for (const line of lines) {
    if (!line.trim()) continue;
    const [ruc, razonSocial, digitoVerificador, rucAnterior, estado] = line.split("|");
    if (!ruc || !razonSocial) continue;
    fs.appendFileSync(outCsv, `${ruc}|${razonSocial}|${digitoVerificador||""}|${rucAnterior||""}|${estado||""}|${fecha}\n`);
  }
}

console.log("Comprimiendo a .gz…");
await new Promise((ok, ko) => {
  const inp = fs.createReadStream(outCsv);
  const out = fs.createWriteStream(outGz);
  inp.on("error", ko); out.on("error", ko);
  out.on("finish", ok);
  inp.pipe(createGzip()).pipe(out);
});

console.log("Listo:", outGz);
