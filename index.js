const fs = require('fs');
const readline = require('readline');
const { zipObject } = require('lodash');

const locationStream = fs.createWriteStream('./scripts/Location');
const serverStream = fs.createWriteStream('./scripts/Server');
const physicalDiskStream = fs.createWriteStream('./scripts/PhysicalDisk');
const logicalDiskStream = fs.createWriteStream('./scripts/LogicalDisk');
const storageStream = fs.createWriteStream('./scripts/Storage');
const CPUStream = fs.createWriteStream('./scripts/CPU');
const priorityStream = fs.createWriteStream('./scripts/Priority');
const databaseStream = fs.createWriteStream('./scripts/Database');
const instanceStream = fs.createWriteStream('./scripts/Instance');
const applicationStream = fs.createWriteStream('./scripts/Application');
const URLStream = fs.createWriteStream('./scripts/URL');
const classificationStream = fs.createWriteStream('./scripts/Classification');
const environmentStream = fs.createWriteStream('./scripts/Environment');

const hostsStream = fs.createWriteStream('./scripts/hosts');
const hasInstanceStream = fs.createWriteStream('./scripts/hasInstance');
const isAllocatedInStream = fs.createWriteStream('./scripts/isAllocatedIn');
const isPartitionedInStream = fs.createWriteStream('./scripts/isPartitionedIn');
const isAttachedToStream = fs.createWriteStream('./scripts/isAttachedTo');
const belongsToStream = fs.createWriteStream('./scripts/belongsTo');
const isPriorityStream = fs.createWriteStream('./scripts/isPriority');
const isInstalledInStream = fs.createWriteStream('./scripts/isInstalledIn');
const managesDataForStream = fs.createWriteStream('./scripts/managesDataFor');
const hasCPUStream = fs.createWriteStream('./scripts/hasCPU');
const isClassifiedStream = fs.createWriteStream('./scripts/isClassified');
const hasStream = fs.createWriteStream('./scripts/has');

const nodeIds = {};
let lineNr;

const createLocations = () => {
  nodeIds.locations = [1, 2, 3, 4];
  locationStream.write(`CREATE (:Location {id:1, name:"NA"})
CREATE (:Location {id:2, name:"LA"})
CREATE (:Location {id:3, name:"EU"})
CREATE (:Location {id:4, name:"CN"})`);
}

const createClassifications = () => {
  nodeIds.classifications = [1, 2, 3];
  classificationStream.write(`CREATE (:Classification {id:1, name:"Confidential"})
CREATE (:Classification {id:2, name:"Highly Confidential"})
CREATE (:Classification {id:3, name:"Restricted"})`);
}

const createStorages = () => {
  nodeIds.storages = [1, 2, 3, 4, 5];
  storageStream.write(`CREATE (:Storage {id:1, name:"S1"})
CREATE (:Storage {id:2, name:"S2"})
CREATE (:Storage {id:3, name:"S3"})
CREATE (:Storage {id:4, name:"S4"})
CREATE (:Storage {id:5, name:"S5"})`);
}

const createPriorities = () => {
  nodeIds.priorities = [1, 2, 3];
  priorityStream.write(`CREATE (:Priority {id:1, name:"Tier1"})
CREATE (:Priority {id:2, name:"Tier2"})
CREATE (:Priority {id:3, name:"Tier3"})`);
}

const createEnvironments = () => {
  environmentStream.write(`CREATE (:Environment {id:1, name:"Production"})
CREATE (:Environment {id:2, name:"Staging"})`);
}

//Genero datos extra
createLocations();
createClassifications();
createStorages();
createEnvironments();
createPriorities();

//Leo archivos
const pickOneRandom = array => array[Math.floor(Math.random() * array.length)];

const handleServerLine = (line) => {
  const header = 'Computerkey|PrincipalNm|EnvironmentNm|ServerFunctionDesc|ServerStatusDesc|ClusterNm|ModelDesc|ManufacturerNm|SystemTypeDesc|ChassisTypeDesc|DNSNm|IPAddr|ForestDnsNm|ActiveDirectorySiteNm';
  const data = zipObject(header.split('|'), line.split('|'));
  // Creo nodo server
  serverStream.write(`CREATE (:Server {computerKey: ${data.Computerkey}, principalNm: "${data.PrincipalNm}", serverFunctionDesc: "${data.ServerFunctionDesc}", serverStatusDesc: "${data.ServerStatusDesc}", clusterNm: "${data.ClusterNm}", modelDesc: "${data.ModelDesc}", manufacturerNm: "${data.ManufacturerNm}", systemTypeDesc: "${data.SystemTypeDesc}", chassisTypeDesc: "${data.ChassisTypeDesc}", DNSNm: "${data.DNSNm}", IPAddr: "${data.IPAddr}", forestDnsNm: "${data.ForestDnsNm}", activeDirectorySiteNm: "${data.ActiveDirectorySiteNm}"})\n`);

  // Asigno location aleatoria
  hostsStream.write(`MATCH (server:Server {computerKey: ${data.Computerkey}}) MATCH(loc:Location {id: ${pickOneRandom(nodeIds.locations)}}) CREATE (loc)-[:hosts]->(server) WITH count(loc) as a\n`);


  // Asigno environment
  if (data.EnvironmentNm) {
    hasStream.write(`MATCH (server:Server {computerKey: ${data.Computerkey}}) MATCH(env:Environment {name: "${data.EnvironmentNm}"}) CREATE (env)-[:has]->(server) WITH count(env) as a\n`);
  }
}

const handleCPULine = (line) => {
  lineNr++;
  const header = 'Computerkey|PrincipalNm|DeviceId|ProcessorNM|SpeedDesc|LogicalProcessorsNbr|ManufacturerNm|CPUKey';
  const data = zipObject(header.split('|'), line.split('|'));
  // Creo nodo CPU
  CPUStream.write(`CREATE (:CPU {id: ${lineNr}, CPUKey: "${data.CPUKey}", deviceId: "${data.DeviceId}", principalNm: "${data.PrincipalNm}", processorNm: "${data.ProcessorNM}", speedDesc: "${data.SpeedDesc}", logicalProcessorsNbr: "${data.LogicalProcessorsNbr}", manufacturerNm: "${data.ManufacturerNm}"})\n`);

  hasCPUStream.write(`MATCH (server:Server {computerKey: ${data.Computerkey}}) MATCH(cpu:CPU {id: ${lineNr}}) CREATE (server)-[:has_cpu]->(cpu) WITH count(cpu) as a\n`);
}

const handleLogicalDiskLine = (line) => {
  lineNr++;
  const header = 'Computerkey|PrincipalNm|VolumeNm|LogicalDiskNm|LogicalDiskDesc|DeviceId|LogicalDiskSizeNbr';
  const data = zipObject(header.split('|'), line.split('|'));

  if (!nodeIds.logicalDisks[data.Computerkey]) {
    nodeIds.logicalDisks[data.Computerkey] = [];
  }

  nodeIds.logicalDisks[data.Computerkey].push(lineNr);

  //Creo nodo LogicalDisk
  logicalDiskStream.write(`CREATE (:LogicalDisk {id: ${lineNr}, principalNm: "${data.PrincipalNm}", volumeNm: "${data.VolumeNm}", logicalDiskNm: "${data.LogicalDiskNm}", logicalDiskDesc: "${data.LogicalDiskDesc}", deviceId: "${data.DeviceId}", logicalDiskSizeNbr: "${data.LogicalDiskSizeNbr}"})\n`);

  //Asigno Storage
  isAllocatedInStream.write(`MATCH(storage:Storage {id: ${pickOneRandom(nodeIds.storages)}}) MATCH(logDisk:LogicalDisk {id: ${lineNr}}) CREATE (logDisk)-[:is_allocated_in]->(storage) WITH count(storage) as a\n`);

  //Asigno Server
  isAttachedToStream.write(`MATCH (server:Server {computerKey: ${data.Computerkey}}) MATCH(logDisk:LogicalDisk {id: ${lineNr}}) CREATE (logDisk)-[:is_attached_to]->(server) WITH count(server) as a\n`);
}

const handlePhysicalDiskLine = (line) => {
  lineNr++;
  const header = 'Computerkey|PrincipalNm|ModelNm|DisplayNm|MediaTypeDesc|DeviceId|PhysicalDiskNm|PhysicalDiskSizeNbr|DiskID';
  const data = zipObject(header.split('|'), line.split('|'));

  //Creo nodo PhysicalDisks
  physicalDiskStream.write(`CREATE (:PhysicalDisk {id: ${lineNr}, diskID: "${data.DiskID}", principalNm: "${data.PrincipalNm}", modelNm: "${data.ModelNm}", displayNm: "${data.DisplayNm}", mediaTypeDesc: "${data.MediaTypeDesc}", deviceId: "${data.DeviceId}", physicalDiskNm: "${data.PhysicalDiskNm}", physicalDiskSizeNbr: "${data.PhysicalDiskSizeNbr}"})\n`);

  //Asigno logical disks
  if (nodeIds.logicalDisks[data.Computerkey]) {
    isPartitionedInStream.write(`MATCH (log:LogicalDisk {id: ${pickOneRandom(nodeIds.logicalDisks[data.Computerkey])}}) MATCH(phys:PhysicalDisk {id: ${lineNr}}) CREATE (phys)-[:is_partitioned_in]->(log) WITH count(phys) as a\n`);
  }
}

const handleDatabaseLine = (line) => {
  lineNr++;
  const header = 'Computerkey|PrincipalNm||InstanceNm|AgentNm|EditionNm|VersionNm|ServicePackVersionDesc|DatabaseNm|OwnerNm|RecoveryModelDesc';
  const data = zipObject(header.split('|'), line.split('|'));

  if (!nodeIds.databases[data.Computerkey]) {
    nodeIds.databases[data.Computerkey] = [];
  }

  nodeIds.databases[data.Computerkey].push({ id: lineNr, instance: data.InstanceNm });

  //Creo nodo Database
  databaseStream.write(`CREATE (:Database {id: ${lineNr}, principalNm: "${data.PrincipalNm}", agentNm: "${data.AgentNm}", editionNm: "${data.EditionNm}", versionNm: "${data.VersionNm}", servicePackVersionDesc: "${data.ServicePackVersionDesc}", databaseNm: "${data.DatabaseNm}", ownerNm: "${data.OwnerNm}", recoveryModelDesc: "${data.RecoveryModelDesc}"})\n`);

  //Creo nodo Instance
  instanceStream.write(`MERGE (:Instance { instanceNm: "${data.InstanceNm}"})\n`);

  //Asigno Database a Classification
  isClassifiedStream.write(`MATCH (class:Classification {id: ${pickOneRandom(nodeIds.classifications)}}) MATCH(db:Database {id: ${lineNr}}) CREATE (db)-[:is_classified]->(class) WITH count(class) as a\n`);

  //Asigno Instance a Server
  hasInstanceStream.write(`MATCH (server:Server {computerKey: ${data.Computerkey}}) MATCH(i:Instance { instanceNm: "${data.InstanceNm}"}) CREATE (server)-[:has_instance]->(i) WITH count(i) as a\n`);

}

const handleApplicationLine = (line) => {
  lineNr++;
  const header = 'Computerkey|PrincipalNm|DisplayNm|IISYearVersionDesc|WebServerDisplayNm|WebServerIISYearVersionDesc|WebSiteDisplayNm|WebSiteIISYearVersionDesc|ApplicationPoolNm|IISWebsiteDesc|ApplicationPoolNm|IISWebsiteUrl|ConnectionTimeoutDesc';
  const data = zipObject(header.split('|'), line.split('|'));

  //Creo nodo Application
  applicationStream.write(`CREATE (:Application {id: ${lineNr}, principalNm: "${data.PrincipalNm}", displayNm: "${data.DisplayNm}", IISYearVersionDesc: "${data.IISYearVersionDesc}", webServerDisplayNm: "${data.WebServerDisplayNm}", webServerIISYearVersionDesc: "${data.WebServerIISYearVersionDesc}", webSiteDisplayNm: "${data.WebSiteDisplayNm}", webSiteIISYearVersionDesc: "${data.WebSiteIISYearVersionDesc}", IISWebsiteDesc: "${data.IISWebsiteDesc}", applicationPoolNm: "${data.ApplicationPoolNm}", connectionTimeoutDesc: "${data.ConnectionTimeoutDesc}"})\n`);

  //Creo nodo URL
  URLStream.write(`CREATE (:URL {id: ${lineNr}, IISWebsiteUrl: "${data.IISWebsiteUrl}"})\n`);

  //Asigno prioridad
  isPriorityStream.write(`MATCH (tier:Priority {id: ${pickOneRandom(nodeIds.priorities)}}) MATCH(app:Application {id: ${lineNr}}) CREATE (app)-[:is_priority]->(tier) WITH count(tier) as a\n`);

  if (nodeIds.databases[data.Computerkey]) {
    const db = pickOneRandom(nodeIds.databases[data.Computerkey]);
    //Asigno Base de datos
    managesDataForStream.write(`MATCH(db:Database {id: ${db.id}}) MATCH(app:Application {id: ${lineNr}}) CREATE (db)-[:manages_data_for]->(app) WITH count(db) as a\n`);

    //Asigno Instancia
    isInstalledInStream.write(`MATCH (instance:Instance {instanceNm: "${db.instance}"}) MATCH(app:Application {id: ${lineNr}}) CREATE (app)-[:is_installed_in]->(instance) WITH count(instance) as a\n`);
  }

  //Asigno URL
  belongsToStream.write(`MATCH (url:URL {id: ${lineNr}}) MATCH(app:Application {id: ${lineNr}}) CREATE (url)-[:belongs_to]->(app) WITH count(app) as a\n`);

}

new Promise((resolve, reject) => {
    const rstream = fs.createReadStream('./CSV/Servers.csv');
    const servers = readline.createInterface({
      input: rstream
    });

    lineNr = 0;
    servers.on('line', handleServerLine);
    rstream.on('end', resolve);
    rstream.on('error', reject);
  }).then(() => new Promise((resolve, reject) => {
    const rstream = fs.createReadStream('./CSV/CPUs.csv');
    const cpus = readline.createInterface({
      input: rstream
    });

    lineNr = 0;
    cpus.on('line', handleCPULine);
    rstream.on('end', resolve);
    rstream.on('error', reject);
  }))
  .then(() => new Promise((resolve, reject) => {
    const rstream = fs.createReadStream('./CSV/LogicalDisks.csv');
    const logicalDisks = readline.createInterface({
      input: rstream
    });

    nodeIds.logicalDisks = {};
    lineNr = 0;
    logicalDisks.on('line', handleLogicalDiskLine);
    rstream.on('end', resolve);
    rstream.on('error', reject);
  }))
  .then(() => new Promise((resolve, reject) => {
    const rstream = fs.createReadStream('./CSV/PhysicalDisks.csv');
    const physicalDisks = readline.createInterface({
      input: rstream
    });

    lineNr = 0;
    physicalDisks.on('line', handlePhysicalDiskLine);
    rstream.on('end', resolve);
    rstream.on('error', reject);
  }))
  .then(() => new Promise((resolve, reject) => {
    const rstream = fs.createReadStream('./CSV/Databases.csv');
    const databases = readline.createInterface({
      input: rstream
    });

    nodeIds.databases = {};
    lineNr = 0;
    databases.on('line', handleDatabaseLine);
    rstream.on('end', resolve);
    rstream.on('error', reject);
  }))
  .then(() => new Promise((resolve, reject) => {
    const rstream = fs.createReadStream('./CSV/Applications.csv');
    const applications = readline.createInterface({
      input: rstream
    });

    lineNr = 0;
    applications.on('line', handleApplicationLine);
    rstream.on('end', resolve);
    rstream.on('error', reject);
  }))
  .then(() => {
    hostsStream.write('RETURN a');
    hasInstanceStream.write('RETURN a');
    isAllocatedInStream.write('RETURN a');
    isPartitionedInStream.write('RETURN a');
    isAttachedToStream.write('RETURN a');
    belongsToStream.write('RETURN a');
    isPriorityStream.write('RETURN a');
    isInstalledInStream.write('RETURN a');
    managesDataForStream.write('RETURN a');
    hasCPUStream.write('RETURN a');
    isClassifiedStream.write('RETURN a');
    hasStream.write('RETURN a');
  })
  .catch(console.error);