var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
const cassandradriver = require('cassandra-driver');
class Cassandra {
    constructor() {
    }
    static init(contactPoints, localDataCenter, keyspace, credentials) {
        this.keyspace = keyspace;
        this.credentials = credentials;
        this.db = new cassandradriver.Client({
            contactPoints,
            localDataCenter,
            keyspace,
            credentials,
        });
        this.status = 'Connection successfully.';
        return ({ status: this.status });
    }
    ;
    static ReadRole() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                this.status = 'Read successfully.';
                yield this.db.execute(`LIST ROLES`).then((result) => {
                    this.report = JSON.stringify(result.rows);
                }).catch((err) => {
                    this.report = JSON.stringify(null);
                    this.status = err.info + " query " + err.query;
                });
            }
            catch (error) {
                this.status = 'error';
            }
            return this.report = JSON.parse(this.report);
        });
    }
    static CreateRole(rolename, superuser, login, password) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                yield this.db.execute(`CREATE ROLE ${rolename} with SUPERUSER = ${superuser} AND LOGIN = ${login} and PASSWORD = '${password}'`).then(() => {
                    this.status = 'CREATE successfully.';
                }).catch((err) => {
                    this.status = err.info + " query " + err.query;
                });
            }
            catch (error) {
                this.status = "error";
            }
            return ({ status: this.status });
        });
    }
    static DeleRole(rolename) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                yield this.db.execute("DROP ROLE " + rolename).then(() => {
                    this.status = 'Erasable successfully.';
                }).catch((err) => {
                    this.status = err.info + " query " + err.query;
                });
            }
            catch (error) {
                this.status = "error";
            }
            return { status: this.status };
        });
    }
    static ReadKeyspace() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                this.status = 'Read successfully.';
                yield this.db.execute(`SELECT * FROM system_schema.keyspaces`).then((result) => {
                    this.report = JSON.stringify(result.rows);
                }).catch((err) => {
                    this.report = JSON.stringify(null);
                    this.status = err.info + " query " + err.query;
                });
            }
            catch (error) {
                this.status = 'error';
            }
            return (this.report = JSON.parse(this.report));
        });
    }
    static CreateKeyspace(identifier, replication) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                yield this.db.execute(`CREATE KEYSPACE ${identifier} WITH replication = {${Object.keys(replication).map((item) => `'${item}' : '${replication[item]}'`)}}`).then(() => {
                    this.status = 'Create successfully.';
                }).catch((err) => {
                    this.status = err.info + " query " + err.query;
                });
            }
            catch (error) {
                this.status = "error";
            }
            return ({ status: this.status });
        });
    }
    static DeleteKeyspace(identifier) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                yield this.db.execute(`DROP KEYSPACE  ${identifier}`).then(() => {
                    this.status = 'Erasable successfully.';
                }).catch((err) => {
                    this.status = err.info + " query " + err.query;
                });
            }
            catch (error) {
                this.status = "error";
            }
            return ({ status: this.status });
        });
    }
    static AlterKeyspace(identifier, replication) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                yield this.db.execute(`ALTER KEYSPACE ${identifier} WITH replication = {${Object.keys(replication).map((item) => `'${item}' : '${replication[item]}'`)}}`).then(() => {
                    this.status = 'ALTER successfully.';
                }).catch((err) => {
                    this.status = err.info + " query " + err.query;
                });
            }
            catch (error) {
                this.status = "error";
            }
            return ({ status: this.status });
        });
    }
    static CreateTable(tablename, colname) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                yield this.db.execute("CREATE TABLE " + this.keyspace + "." + tablename + " (id UUID PRIMARY KEY ," + colname.map((x) => `${x}`).join(', ') + ",created_at timestamp,updated_at timestamp)").then(() => {
                    this.status = 'CREATE successfully.';
                }).catch((err) => {
                    this.status = err.info + " query " + err.query;
                });
            }
            catch (error) {
                this.status = "error";
            }
            return ({ status: this.status });
        });
    }
    static AlterTable(tablename, colname) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                yield this.db.execute("ALTER TABLE " + this.keyspace + "." + tablename + " ADD (" + colname.map((x) => `${x}`).join(', ') + ")").then(() => {
                    this.status = 'ALTER successfully.';
                }).catch((err) => {
                    this.status = err.info + " query " + err.query;
                });
            }
            catch (error) {
                this.status = "error";
            }
            return ({ status: this.status });
        });
    }
    static ClearTable(tablename) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                yield this.db.execute("TRUNCATE " + this.keyspace + "." + tablename).then(() => {
                    this.status = 'Clear successfully.';
                }).catch((err) => {
                    this.status = err.info + " query " + err.query;
                });
            }
            catch (error) {
                this.status = "error";
            }
            return ({ status: this.status });
        });
    }
    static DeleTable(tablename) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                yield this.db.execute("DROP TABLE " + this.keyspace + "." + tablename).then(() => {
                    this.status = 'Erasable successfully.';
                }).catch((err) => {
                    this.status = err.info + " query " + err.query;
                });
            }
            catch (error) {
                this.status = "error";
            }
            return ({ status: this.status });
        });
    }
    static CreateData(tablename, queries) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const Uuid = require('cassandra-driver').types.Uuid;
                let dataall = [];
                Object.keys(queries).map((item) => {
                    let id = Uuid.random();
                    dataall.push({
                        query: `INSERT INTO ${this.keyspace}.${tablename} (id,` + queries[item]['query'].map((data) => `${data}`).join(',') + `,created_at,updated_at) VALUES (${id},?,?,${Date.now()},${Date.now()})`,
                        params: queries[item].params
                    });
                });
                yield this.db.batch(dataall, { prepare: true }).then(() => {
                    this.status = 'Create successfully.';
                }).catch((err) => {
                    this.status = err.info + " query " + err.query;
                });
            }
            catch (error) {
                this.status = "error";
            }
            return ({ status: this.status });
        });
    }
    static BatchData(queries) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                yield this.db.batch(queries, { prepare: true }).then(() => {
                    this.status = 'Batch successfully.';
                }).catch((err) => {
                    this.status = err.info + " query " + err.query;
                });
            }
            catch (error) {
                this.status = "error";
            }
            return ({ status: this.status });
        });
    }
    static ReadData(tablename, colname, condition, limit) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                (limit == null) ? limit = 1 : limit = limit;
                this.status = 'Read successfully.';
                if (condition == null) {
                    yield this.db.execute(`SELECT ` + colname.map((x) => `${x}`).join(',') + ` FROM ${this.keyspace}.${tablename} limit ${limit}`).then((result) => {
                        this.report = JSON.stringify(result.rows);
                    }).catch((err) => {
                        this.report = JSON.stringify(null);
                        this.status = err.info + " query " + err.query;
                    });
                }
                else {
                    yield this.db.execute(`SELECT ` + colname.map((x) => `${x}`).join(',') + ` FROM ${this.keyspace}.${tablename} WHERE ${Object.keys(condition).map((x) => `${x} = ${condition[x]}`).join("AND")} limit ${limit} ALLOW FILTERING`).then((result) => {
                        this.report = JSON.stringify(result.rows);
                    }).catch((err) => {
                        this.report = JSON.stringify(null);
                        this.status = err.info + " query " + err.query;
                    });
                }
            }
            catch (error) {
                this.status = 'error';
            }
            return (this.report = JSON.parse(this.report));
        });
    }
    static ReadDataCustomer(queries) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                this.status = 'Read successfully.';
                yield this.db.execute(`${queries}`).then((result) => {
                    this.report = JSON.stringify(result.rows);
                }).catch((err) => {
                    this.report = JSON.stringify(null);
                    this.status = err.info + " query " + err.query;
                });
            }
            catch (error) {
                this.status = 'error';
            }
            return (this.report = JSON.parse(this.report));
        });
    }
    static UpdateData(tablename, set, condition) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const query = `UPDATE ${this.keyspace}.${tablename}  SET ` + Object.keys(set).map((data) => `${data} = ?`).join(' , ') + ` ,updated_at = ${Date.now()} WHERE ${Object.keys(condition).map((data) => `${data} = ?`).join(' AND ')}`;
                const params = Object.keys(set).map((data) => (set[data]));
                const paramsB = Object.keys(condition).map((data) => (condition[data]));
                Array.prototype.push.apply(params, paramsB);
                console.log(params);
                yield this.db.execute(query, params, { prepare: true }).then(() => {
                    this.status = 'Update successfully.';
                }).catch((err) => {
                    console.log(err);
                    this.status = err.info + " query " + err.query;
                });
            }
            catch (error) {
                this.status = "error";
            }
            console.log(this.status);
            return ({ status: this.status });
        });
    }
    static DeleteData(tablename, condition) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const query = `DELETE FROM ${this.keyspace}.${tablename} WHERE ${Object.keys(condition).map((data) => `${data} = ?`).join(' AND ')}`;
                const params = Object.keys(condition).map((data) => (condition[data]));
                yield this.db.execute(query, params, { prepare: true }).then(() => {
                    this.status = 'Erasable successfully.';
                }).catch((err) => {
                    this.status = err.info + " query " + err.query;
                });
            }
            catch (error) {
                this.status = "error";
            }
            return ({ status: this.status });
        });
    }
}
module.exports = {
    Cassandra
};
