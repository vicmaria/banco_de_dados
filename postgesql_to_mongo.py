import pymongo
from pymongo import MongoClient
import psycopg2

#Conecta com o mongo
def connecta() -> pymongo.database.Database:
   client = MongoClient('localhost:27017')
   db = client.empresax
   return db

#conecta com o postgresql
def postgres() -> psycopg2.extensions.connection:
   con = psycopg2.connect(
       host="127.0.0.1",
       database="empresax",
       user="user",
       password="password")
   return con

#retorna o cursor do postgre
def cursor(con) -> psycopg2.extensions.cursor:
   cur = con.cursor()
   return cur

#busca a tabela de clientes
def tb_cliente(cur):
   db = connecta()
   cur.execute("select * from vendas.tb_cliente")
   rows = cur.fetchall()
   for r in rows:
       id = int(r[0])
       titulo = str(r[1])
       nome = str(r[2])
       sobrenome = str(r[3])
       endereco = str(r[4])
       num = str(r[5])
       complemento = str(r[6])
       cep = str(r[7])
       cidade = str(r[8])
       estado = str(r[9])
       fixo = str(r[10])
       movel = str(r[11])
       fg = int(r[12])
       db.cliente.insert_one(
           {"id_cliente": id, "titulo": titulo, "nome": nome, "sobrenome": sobrenome,
            "endereco": endereco, "numero": num, "complemento": complemento, "cep": cep,
            "cidade": cidade, "estado": estado, "fone_fixo": fixo, "fone_movel": movel,
            "fg_ativo": fg})

#busca a tabela de itens e retorna uma lista de ids
def tb_item(cur) -> list:
   db = connecta()
   cur.execute("select * from vendas.tb_item")
   rows = cur.fetchall()
   ids_item = list()
   for r in rows:
       id = int(r[0])
       ds = str(r[1])
       venda = float(r[3])
       custo = float(r[2])
       fg = int(r[4])
       ids_item.append(id)
       db.item.insert_one(
           {"id_item": id, "ds_item": ds, "preco_custo": custo, "preco_venda": venda,
            "fg_ativo": fg})
   return ids_item

#busca a tabela de estoque
def tb_estoque(cur):
   db = connecta()
   cur.execute("select * from vendas.tb_estoque")
   rows = cur.fetchall()
   for r in rows:
       id = int(r[0])
       quantidade = int(r[1])
       db.estoque.insert_one({"id_item": id, "quantidade": quantidade})

#busca a tabela de estoque a adiciona ao item
def tb_estoque_item(ids_item, cur):
   db = connecta()
   for id in ids_item:
       cur.execute("SELECT quantidade FROM vendas.tb_estoque  WHERE id_item = %s", (id,))
       rows = cur.fetchall()
       for r in rows:
           quantidade = int(r[0])
           db.item.update_one({"id_item": id},
                                {"$push": {"quantidade": quantidade}})

#busca a tabela de codigo de barras a adiciona ao item
def tb_codigo_item(ids_item, cur):
   db = connecta()
   for id in ids_item:
       cur.execute("SELECT codigo_barras FROM vendas.tb_codigo_barras  WHERE id_item = %s", (id,))
       rows = cur.fetchall()
       for r in rows:
           codigo = int(r[0])
           db.item.update_one({"id_item": id},
                              {"$push": {"codigo_barras": codigo}})

#busca a tabela de codigo de barras
def tb_codigo_barras(cur):
   db = connecta()
   cur.execute("select * from vendas.tb_codigo_barras")
   rows = cur.fetchall()
   for r in rows:
       codigo = int(r[0])
       id = int(r[1])
       db.codigo_barras.insert_one({"codigo_barras": codigo, "id_item": id})

#busca a tabela de pedido e retorna uma lista de ids
def tb_pedido(cur) -> list:
   db = connecta()
   cur.execute("select * from vendas.tb_pedido")
   rows = cur.fetchall()
   id_pedido = list()
   for r in rows:
       id = int(r[0])
       idc = int(r[1])
       compra = str(r[2])
       entrega = str(r[3])
       valor = float(r[4])
       fg = int(r[5])
       id_pedido.append(id)
       db.pedido.insert_one(
           {"id_pedido": id, "id_cliente": idc, "dt_compra": compra, "dt_entrega": entrega, "valor": valor,
            "fg_ativo": fg})
   return id_pedido

#relaciona item e pedido
def tb_item_pedido(id_pedido,cur):
   db = connecta()
   for id in id_pedido:
       cur.execute("SELECT id_item FROM vendas.tb_item_pedido  WHERE id_pedido = %s", (id,))
       rows = cur.fetchall()
       for r in rows:
           item = int(r[0])
           db.pedido.update_one({"id_pedido": id},
                         {"$push": {"itens": item}}
       )
def main():

   con = postgres()

   cur = cursor(con)

   tb_cliente(cur)

   ids_item = tb_item(cur)

   #tb_estoque(cur)

   tb_estoque_item(ids_item, cur)

   tb_codigo_item(ids_item, cur)

   #tb_codigo_barras(cur)

   id_pedido = tb_pedido(cur)

   tb_item_pedido(id_pedido, cur)
   cur.close()

   # close the connection
   con.close()


if __name__ == '__main__':
   main()
