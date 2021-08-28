# Rox_Teste

## O problema 
    O presente problema se refere aos dados de uma empresa que produz bicicletas. 
    
    O objetivo deste desafio é compreender os seus conhecimentos e experiência analisando os seguintes aspectos:
    
    1. Fazer a modelagem conceitual dos dados;
    2. Criação da infraestrutura necessária;
    3. Criação de todos os artefatos necessários para carregar os arquivos para o banco criado;
    4. Desenvolvimento de SCRIPT para análise de dados;
    5. (opcional) Criar um relatório em qualquer ferramenta de visualização de dados.
    
Os seguintes arquivos devem ser importados (ETL) para o banco de dados de sua escolha: 

    • Person.Person.csv
    • Production.Product.csv
    • Sales.Customer.csv
    • Sales.SalesOrderDetail.csv
    • Sales.SalesOrderHeader.csv
    • Sales.SpecialOfferProduct.csv

### Person.Person

Iniciei a exploração das bases pelo banco Person.Person.csv.

![GitHub Logo](/Images/missing_person0.png)

Três variáveis possuem mais de 90% de missing, decidi retirá-las da base. A variável **MiddleName** possui 42,55% de missing, para não perder esta variável eu preenchi as células vazias com o último nome do cliente.

Todas as variáveis estão como string (em todos os arquivos) as variáveis de preço, valores inteiros relacionados a quantidade e datas foram alterados tiveram seus tipos alterados. A **Demographics** é uma string com algumas informações sobre o cliente, tais como: data de nascimento, estado civil, renda, sexo etc. Como pode ser visto na próxima figura

![GitHub Logo](/Images/demo_row.png)

Após algumas manipulações, utilizando o pandas, criei estas novas variáveis: TotalPurchase, DateFirstPurchase, BirthDate, MaritalStatus, YearlyIncome, Gender, TotalChildren, NumberChildrenAtHome, Education, Occupation, HomeOwnerFlag, NumberCarsOwned e CommuteDistance. Contudo, alguns clientes não possuem algumas dessas informações. 

obs: não tratei os missings das colunas DateFirstPurchase e BirthDate.

Os missings dessas variáveis restantes foram tratados da seguinte forma:

 * TotalPurchase: preenchi com o valor médio.
 * TotalChildren, NumberChildrenAtHome e NumberCarsOwned: preenchi com a moda.
 * MaritalStatus, YearlyIncome, Gender, Education, Occupation, HomeOwnerFlag e CommuteDistance: Essas colunas são categóricas e preenchi os missings com 'undefined'.

![GitHub Logo](/Images/missing_person1.png)

Para fins de limpeza poderia exluir as linhas com missing nas colunas MaritalStatus e YearlyIncome, mas não fiz isso. Para não perder as variáveis BirthDate e DateFirstPurchase, pode ser feita alguma imputação. Excluir as linhas parece ser algo complicado por legaria 7% da base.

Esta primeira parte foi feita em pandas. Para uma base maior isso pode ser problemático pq demoraria muito. No processo de criação das variáveis a partir da variável **Demographics** eu usei dois loops, isso tb é complicado pq o código fica lento. Seria necessário pensar uma forma mais eficiente de fazer as mudanças.

Eu abri o banco Person.Person em pandas criei um novo dataframe com as novas variáveis e mesma quantidade de linhas e juntei com o arquivo original.
Tentei transforma o pandas dataframe em pyspark dataframe mas deu erro. Não procurei uma solução e optei por salvá-lo e abrir o arquivo com pyspark.

Nesta segunda parte eu importo o banco Person.Person com novas features e os outros bancos utilizando spark. 

### Production.Product

Como já falei sobre a organização e limpeza do banco Person.Person, o proximo banco analisado foi **Production.Product**

![GitHub Logo](/Images/product0.png)

![GitHub Logo](/Images/product1.png)

Como as queries que são pedidas não envolvem as características dos produtos optei por não tratar esses missings. Nesse banco alterei apenas o formato das datas e coloquei em float as variáveis de preço.

### Sales.Customer

Este banco possui os dados sobre as as lojas.

![GitHub Logo](/Images/store00.png)

![GitHub Logo](/Images/store0.png)

Poderia ter retirado a coluna StoreID, mas decidi não fazer isso. Por fim, alterei apenas o formato da coluna ModifiedDate.

### Sales.SalesOrderDetail

![GitHub Logo](/Images/sales0.png)

![GitHub Logo](/Images/sales1.png)

### Sales.SalesOrderHeader

![GitHub Logo](/Images/header0.png)

![GitHub Logo](/Images/header1.png)

### Sales.SpecialOfferProduct

![GitHub Logo](/Images/offer0.png)

![GitHub Logo](/Images/offer1.png)


## Análise de dados

##### Com base na solução implantada responda aos seguintes questionamentos:

1.	**Escreva uma query que retorna a quantidade de linhas na tabela Sales.SalesOrderDetail pelo campo SalesOrderID, desde que tenham pelo menos três linhas de detalhes.**

    SalesDetail.select('SalesOrderID').count()
    
    ou
    
    'select count(SalesOrderID) as N_linhas from SalesDetail'


2.	Escreva uma query que ligue as tabelas Sales.SalesOrderDetail, Sales.SpecialOfferProduct e Production.Product e retorne os 3 produtos (Name) mais vendidos (pela soma de OrderQty), agrupados pelo número de dias para manufatura (DaysToManufacture).

    "select first(production.Name), sum(SalesDetail.OrderQty) as sum_OrderQty  \
          from production LEFT JOIN SpecialOfferProduct ON production.ProductID = SpecialOfferProduct.ProductID \
          LEFT JOIN SalesDetail ON SpecialOfferProduct.SpecialOfferID = SalesDetail.SalesOrderDetailID \
          group by production.DaysToManufacture order by sum_OrderQty desc limit 3"
          
 ![GitHub Logo](/Images/sales0.png)


3.	Escreva uma query ligando as tabelas Person.Person, Sales.Customer e Sales.SalesOrderHeader de forma a obter uma lista de nomes de clientes e uma contagem de pedidos efetuados.


4.	Escreva uma query usando as tabelas Sales.SalesOrderHeader, Sales.SalesOrderDetail e Production.Product, de forma a obter a soma total de produtos (OrderQty) por ProductID e OrderDate.


5.	Escreva uma query mostrando os campos SalesOrderID, OrderDate e TotalDue da tabela Sales.SalesOrderHeader. Obtenha apenas as linhas onde a ordem tenha sido feita durante o mês de setembro/2011 e o total devido esteja acima de 1.000. Ordene pelo total devido decrescente.


