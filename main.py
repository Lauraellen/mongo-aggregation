from db.database import Database
from helper.WriteAJson import writeAJson
from dataset.pessoa_dataset import dataset as pessoa_dataset
from dataset.produto_database import dataset as produto_dataset

compras = Database(database="database", collection="produtos", dataset=produto_dataset)
#compras.resetDatabase()

pessoas = Database(database="database", collection="pessoas", dataset=pessoa_dataset)
# pessoas.resetDatabase()

result1 = compras.collection.aggregate([
    {"$lookup":
        {
            "from": "pessoas",
            "localField": "cliente_id",
            "foreignField": "_id",
            "as": "pessoa"
        },
    },
    {"$group": {"_id": "$pessoa", "total": {"$sum": "$total"}}},
    {"$sort": {"total": -1}},
    {"$unwind": '$_id'},
    {"$project": {
        "_id": 0,
        "nome": "$_id.nome",
        "total": 1,
        "desconto": {
            "$cond": {"if": {"$gte": ["$total", 10]}, "then": "true", "else": "false"}
        },

    }},


])

writeAJson(result1, "result1")



