import psycopg2
global conn
global cursor


def disconnect():
    global conn, cursor
    ret = True

    try:
        cursor.close()
        conn.close()
        print("Encerrando a conexão...")
    except psycopg2.OperationalError:
        print("Erro ao se desconectar da base de dados!")
        ret = False

    return ret


def connect(dbname, user, password):
    global conn, cursor
    ret = True

    try:
        conn_string = f"dbname={dbname} user={user} password={password}"
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
    except psycopg2.OperationalError:
        print("Erro ao se conectar a base de dados!")
        ret = False

    return ret


def print_data():
    global cursor
    column_names = [desc[0] for desc in cursor.description]
    print("")
    print(column_names)

    ret = cursor.fetchall()
    for line in ret:
        print(line)
    print("")


# Nome do cliente, quantidade de pedidos e valor gasto de clientes que gastaram mais de {vlr_gasto} reais em produtos:
def consulta1(vlr_gasto):
    query = "SELECT Pessoas.Nome, COUNT(DISTINCT DetalhesPedido.idPedido) AS quantidadePedidos, "
    query += "SUM(Produtos.valor * ItensPedido.quantidade) AS valorGasto "
    query += "FROM DetalhesPedido "
    query += "JOIN Clientes ON cliente = idCliente "
    query += "JOIN Pessoas ON pessoa = idPessoa "
    query += "JOIN ItensPedido ON pedido = idPedido "
    query += "JOIN Produtos ON item = idProduto "
    query += "GROUP BY Pessoas.idPessoa "
    query += f"HAVING SUM(Produtos.valor * ItensPedido.quantidade) > {vlr_gasto};"
    return query


# Nome do fornecedor, nome do produto fornecido, quantidade de vezes que esse produto foi fornecido
# e quantidade de itens fornecidos por fornecimento:
def consulta2():
    query = "SELECT Fornecedor.nome AS fornecedor, Produtos.nome AS produtoFornecido, "
    query += "COUNT(Fornecimento.idFornecimento) AS vezesFornecido, SUM(Fornecimento.quantidade) AS quantidadeFornecida"
    query += " FROM Fornecedor "
    query += "JOIN Fornecimento ON fornecedor = idFornecedor "
    query += "JOIN Produtos ON idProduto = produto "
    query += "GROUP BY Fornecedor.nome, Produtos.nome"
    return query


# O nome dos clientes que fizeram pedidos no dia={dia} e que não precisaram de entrega:
def consulta3(data):
    query = "SELECT Pessoas.nome AS Cliente "
    query += "FROM Pessoas "
    query += "WHERE idpessoa IN (SELECT cliente "
    query += "                  FROM DetalhesPedido "
    query += f"                  WHERE datapedido = '{data}' AND idpedido NOT IN "
    query += "                                      (SELECT identrega FROM entrega));"

    return query


# O nome dos produtos que têm preço acima da média, são fornecidos pela ID = {id}
# e que não foram vendidos de forma online
def consulta4(id_fornecedor):
    query = "SELECT Produtos.nome AS produto "
    query += "FROM Fornecedor "
    query += "JOIN Fornecimento ON Fornecedor.idFornecedor = Fornecimento.fornecedor "
    query += "JOIN Produtos ON Fornecimento.produto = Produtos.idProduto "
    query += f"WHERE {id_fornecedor} = 1 AND Valor > (SELECT AVG(valor) "
    query += "                                   FROM Produtos"
    query += "                                   JOIN ItensPedido ON idProduto = item "
    query += "                                   JOIN DetalhesPedido ON ItensPedido.pedido = DetalhesPedido.idpedido "
    query += "                                   WHERE DetalhesPedido.idpedido NOT IN (SELECT identrega FROM entrega));"
    return query


# Selecionar todos os clientes que nunca fizeram um pedido com algum item que custa acima de R$ {valor}
def consulta5(valor):
    query = "SELECT nome FROM pessoas c "
    query += "JOIN clientes ON c.idpessoa = clientes.pessoa "
    query += "WHERE NOT EXISTS (SELECT * FROM produtos "
    query += "                  JOIN itensPedido ON produtos.idproduto = itenspedido.item "
    query += "                  JOIN detalhesPedido ON itenspedido.pedido = detalhespedido.idpedido "
    query += f"                  WHERE idcliente = c.idpessoa AND produtos.valor > {valor});"

    return query


# Número do pedido e nome do cliente que residem no {estado} e tiveram suas compras entregues
# em menos de {num_dias} dias:
def consulta6(estado, num_dias):
    query = "SELECT DISTINCT HistoricoCompras.idPedido, HistoricoCompras.nomeCliente "
    query += "FROM HistoricoCompras "
    query += "JOIN Pessoas ON HistoricoCompras.CPFCliente = Pessoas.CPF "
    query += "JOIN Endereco ON endereco = idEndereco "
    query += f"WHERE ((HistoricoCompras.dataEntrega - HistoricoCompras.dataPedido) < {num_dias}) "
    query += f"AND Endereco.UF = '{estado}';"
    return query


# UF do estado e quantidade de vendas no seu respectivo estado, ordenado do com mais vendas, para o com menos:
def consulta7():
    query = "SELECT Endereco.UF, COUNT(DISTINCT HistoricoCompras.idPedido) "
    query += "FROM HistoricoCompras "
    query += "JOIN Pessoas ON HistoricoCompras.nomeCliente = Pessoas.nome "
    query += "JOIN Endereco ON endereco = idEndereco "
    query += "GROUP BY Endereco.UF "
    query += "ORDER BY COUNT(DISTINCT HistoricoCompras.idPedido) DESC;"
    return query


# Lista de entregadores que fizeram pelo menos 1 entrega exibindo nome e quantidade de entrega.
def consulta8():
    query = "SELECT Pessoas.nome, COUNT(Entregador.entrega) as QuantidadeEntregas "
    query += "FROM Pessoas "
    query += "JOIN Funcionarios ON Pessoas.idpessoa = Funcionarios.pessoa "
    query += "JOIN Entregador ON Funcionarios.idFuncionario = Entregador.funcionario "
    query += "GROUP BY Entregador.funcionario, Pessoas.nome "
    query += "ORDER BY COUNT(Entregador.entrega) DESC;"
    return query


# ID do pedido, o nome do produto, e a data do pedido com maior quantidade do mesmo produto
def consulta9():
    query = "SELECT DetalhesPedido.idpedido, nome, DetalhesPedido.datapedido "
    query += "FROM ItensPedido "
    query += "JOIN DetalhesPedido ON ItensPedido.pedido = DetalhesPedido.idPedido "
    query += "JOIN Produtos ON ItensPedido.item = Produtos.idProduto "
    query += "WHERE ItensPedido.quantidade = (SELECT MAX(quantidade) "
    query += "FROM ItensPedido);"
    return query


# Lojas do estado {uf} e que já tiveram pedidos
def consulta10(uf):
    query = "SELECT nome FROM Lojas "
    query += "JOIN Endereco On Lojas.endereco = endereco.idendereco "
    query += f"WHERE endereco.UF = '{uf}' AND Lojas.idloja IN (Select loja From DetalhesPedido);"
    return query


# conn.psycopg2.connect() - Conecta ao banco de dados
# cursor = conn.cursor - Abre um cursor para fazer operações no banco
# cursor.execute("COMANDO SQL") - Executa um comando sql
# cursor.fetchone() - Usado para resgatar dados depois de um comando sql que retorne dados (SELECT)
# conn.commit() - Persiste alterações no banco de dados
# cursor.close() - Fecha comunicacão com o cursor
# conn.close() - Fecha comunicação com o banco
def main():
    global cursor
    dbname = "ArtigosEsportivosDB"
    user = "postgres"
    password = "1q2w3e4r5t"
    exec_consultas = True
    nomes_cmd = ["Consulta1",
                 "Consulta2",
                 "Consulta3",
                 "Consulta4",
                 "Consulta5",
                 "Consulta6",
                 "Consulta7",
                 "Consulta8",
                 "Consulta9",
                 "Consulta10",
                 "Encerrar comunicação"]

    connection_success = connect(dbname, user, password)

    if connection_success:
        while exec_consultas:
            for i in range(0, len(nomes_cmd)):
                print(f"{i + 1} - {nomes_cmd[i]}")
            cmd = int(input("Qual comando você deseja executar? "))

            if cmd == 1:
                print("\nDescrição: Nome do cliente, quantidade de pedidos e valor gasto de clientes que gastaram "
                      "mais de {vlr_gasto} reais em produtos")
                vlr_gasto = float(input("Valor a ser consultado: "))
                cursor.execute(consulta1(vlr_gasto))
                print_data()

            elif cmd == 2:
                print("Descrição: Nome do fornecedor, nome do produto fornecido, quantidade de vezes que esse "
                      "produto foi fornecido e quantidade de itens fornecidos por fornecimento")
                cursor.execute(consulta2())
                print_data()

            elif cmd == 3:
                print("Descrição: O nome dos clientes que fizeram pedidos no dia={dia} e "
                      "que não precisaram de entrega:")
                data_dia = input("Dia (formato XX): ")
                data_mes = input("Mes (formato XX): ")
                data_ano = input("Ano (formato XXXX): ")
                data = f"{data_ano}-{data_mes}-{data_dia}"
                cursor.execute(consulta3(data))
                print_data()

            elif cmd == 4:
                print("Descrição: O nome dos produtos que têm preço acima da média, são fornecidos pela ID = {id} "
                      "e que não foram vendidos de forma online")
                id_fornecedor = input("Id do fornecedor: ")
                cursor.execute(consulta4(id_fornecedor))
                print_data()

            elif cmd == 5:
                print("Descrição: Selecionar todos os clientes que nunca fizeram um pedido com "
                      "algum item que custa acima de R$ {valor}")
                valor = input("Valor do item: ")
                cursor.execute(consulta5(valor))
                print_data()

            elif cmd == 6:
                print("Descrição: Número do pedido e nome do cliente que residem no {estado} e tiveram suas "
                      "compras entregues em menos de {num_dias} dias")
                estado = input("UF do estado a ser consultado: ")
                num_dias = input("Número de dias a ser consultado: ")
                cursor.execute(consulta6(estado, num_dias))
                print_data()

            elif cmd == 7:
                print("Descrição: UF do estado e quantidade de vendas no seu respectivo estado, "
                      "ordenado do com mais vendas, para o com menos")
                cursor.execute(consulta7())
                print_data()

            elif cmd == 8:
                print(" Descrição: Lista de entregadores que fizeram pelo menos 1 entrega exibindo nome e "
                      "quantidade de entrega.")
                cursor.execute(consulta8())
                print_data()

            elif cmd == 9:
                print("Descrição: ID do pedido, o nome do produto, e a data do pedido com maior"
                      " quantidade do mesmo produto")
                cursor.execute(consulta9())
                print_data()

            elif cmd == 10:
                print("Descrição: Lojas do estado {uf} e que já tiveram pedidos ")
                uf = input("UF do estado: ")
                cursor.execute(consulta10(uf))
                print_data()

            elif cmd == 11:
                if disconnect():
                    exec_consultas = False


if __name__ == "__main__":
    main()
