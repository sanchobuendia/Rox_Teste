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
    
###

    Na rimeira parte, fiz a análise de cada banco separadamente. Nesta ana1lise eu altero o formato de algumas
    variáveis para float, int ou date, faço a imputação de valores faltantes e retiro colunas com muitos missings.

### Person.Person

Iniciei a exploração das bases pelo banco Person.Person.csv.

![GitHub Logo](/Images/missing_person0.png)

Três variáveis possuem mais de 90% de missing, decidi retirá-las da base. A variável **MiddleName** possui 42,55% de missing, para não perder esta variável eu preenchi as células vazias com o último nome do cliente.

Todas as variáveis estão como string (em todos os arquivos) as variáveis de preço, valores inteiros relacionados a quantidade e datas foram alterados tiveram seus tipos alterados. A **Demographics** é uma string com algumas informações sobre o cliente, tais como: data de nascimento, estado civil, renda, sexo etc. Como pode ser visto na próxima figura

![GitHub Logo](/Images/demo_row.png)

Após algumas manipulações, utilizando o pandas, criei estas novas variáveis: TotalPurchase, DateFirstPurchase, BirthDate, MaritalStatus, YearlyIncome, Gender, TotalChildren, NumberChildrenAtHome, Education, Occupation, HomeOwnerFlag, NumberCarsOwned, CommuteDistance, DateFirstPurchase e BirthDate. Contudo, alguns clientes não possuem algumas dessas informações.

Os missings dessas variáveis restantes foram tratados da seguinte forma:

 * TotalPurchase: preenchi com o valor médio.
 * TotalChildren, NumberChildrenAtHome e NumberCarsOwned: preenchi com a moda.
 * MaritalStatus, YearlyIncome, Gender, Education, Occupation, HomeOwnerFlag, CommuteDistance, MaritalStatus e YearlyIncome: Essas colunas são categóricas e preenchi os missings com 'undefined'.
 * DateFirstPurchase e BirthDate: Neste caso eu optei por preencher com a data mais frequente dentro da distribuição. Acredito que isso não enviezaria futuras análises.

![GitHub Logo](/Images/personfinal.png)


Esta primeira parte foi feita em pandas. Para uma base maior isso pode ser problemático pq demoraria muito. No processo de criação das variáveis a partir da variável **Demographics** eu usei dois loops, isso tb é complicado pq o código fica lento. Seria necessário pensar uma forma mais eficiente de fazer as mudanças.

Eu abri o banco Person.Person em pandas criei um novo dataframe com as novas variáveis e mesma quantidade de linhas e juntei com o arquivo original.
Tentei transforma o pandas dataframe em pyspark dataframe mas deu erro. Não procurei uma solução e optei por salvá-lo e abrir o arquivo com pyspark.

Nesta segunda parte eu importo o banco Person.Person com novas features e os outros bancos utilizando spark. 

### Production.Product

Como já falei sobre a organização e limpeza do banco Person.Person, o proximo banco analisado foi **Production.Product**

![GitHub Logo](/Images/product0.png)

    * Inicialmente eu retirei as colunas DiscontinuedDate e SellEndDate por possuírem mais de 80% de missings.
    * As variáveis ProductModelID e ProductSubcategoryID são códigos. Diante da dificuldade em fazer a imputação
        de códigos desconhecidos eu retirei essas colunas.
    * A Weight representa o peso do produto. Retirei essa coluna.
    * As coutras colunas com missing são categóriacas e algumas células estão preenchidas com a string "NULL". 
        Eu substituí essa string pela string 'undefined'.

![GitHub Logo](/Images/product1.png)

![GitHub Logo](/Images/productionfinal.png)


### Sales.Customer

Este banco possui os dados sobre as as lojas.

![GitHub Logo](/Images/store00.png)

![GitHub Logo](/Images/store0.png)

![GitHub Logo](/Images/salescostumerfinal.png)

A coluna StoreID tem 93.26% de missing, decidi retirá-la. Por fim, alterei apenas o formato da coluna ModifiedDate.

### Sales.SalesOrderDetail

![GitHub Logo](/Images/sales0.png)

![GitHub Logo](/Images/sales1.png)

![GitHub Logo](/Images/salesdetailfinal.png)

Neste banco os preços vieram com "," o que estava causando erro ao transformar para float. Pra solucionar este problema 
troquei a vírgula por ponto e depois trasnformei para float. Aqui tb troquei o formato das datas de string para datetime.

A coluna CarrierTrackingNumber possui 49.79% das células com a string "NULL" esse código não seria útil em análises estatísticas e poderia ser retirada da base, mas decidi substituir a string "NULL" por 'undefined'.

### Sales.SalesOrderHeader

![GitHub Logo](/Images/header0.png)

![GitHub Logo](/Images/header1.png)

As colunas Comment, PurchaseOrderNumber e SalesPersonID foram retiradas pq tem mais de 80% de missing.
As colunas CurrencyRateID, CreditCardApprovalCode e CreditCardID são categóricas e possuem células com a string "NULL", essa string foi trocada por 'undefined'.

![GitHub Logo](/Images/salesheaderfinal.png)

### Sales.SpecialOfferProduct

![GitHub Logo](/Images/offer0.png)

![GitHub Logo](/Images/offer1.png)

Este banco não possui problemas na estrutura. Por fim, alterei apenas o formato da coluna ModifiedDate para datetime.

![GitHub Logo](/Images/salesofferfinal.png)


## Análise de dados

##### Com base na solução implantada responda aos seguintes questionamentos:

1.	**Escreva uma query que retorna a quantidade de linhas na tabela Sales.SalesOrderDetail pelo campo SalesOrderID, desde que tenham pelo menos três linhas de detalhes.**

    SalesDetail.select('SalesOrderID').count()
    
    ou
    
    'select count(SalesOrderID) as N_linhas from SalesDetail'


2.	Escreva uma query que ligue as tabelas Sales.SalesOrderDetail, Sales.SpecialOfferProduct e Production.Product e retorne os 3 produtos (Name) mais vendidos (pela soma de OrderQty), agrupados pelo número de dias para manufatura (DaysToManufacture).

```sql
"select first(production.Name), sum(SalesDetail.OrderQty) as sum_OrderQty  
 from production LEFT JOIN SpecialOfferProduct ON production.ProductID = SpecialOfferProduct.ProductID 
 LEFT JOIN SalesDetail ON SpecialOfferProduct.SpecialOfferID = SalesDetail.SalesOrderDetailID 
 group by production.DaysToManufacture 
 order by sum_OrderQty desc 
 limit 3"
```
 ![GitHub Logo](/Images/Q1.png)


3.	Escreva uma query ligando as tabelas Person.Person, Sales.Customer e Sales.SalesOrderHeader de forma a obter uma lista de nomes de clientes e uma contagem de pedidos efetuados.

```sql
"select CONCAT(person.FirstName,' ', person.LastName) as Name, count(SalesHeader.SalesOrderID) as Qtd_orders 
from SalesHeader LEFT JOIN SalesCustomer ON SalesCustomer.CustomerID = SalesHeader.CustomerID
LEFT JOIN person ON person.BusinessEntityID = SalesCustomer.CustomerID 
group by person.FirstName, person.MiddleName, person.LastName"
```
![GitHub Logo](/Images/Q3.png)
4.	Escreva uma query usando as tabelas Sales.SalesOrderHeader, Sales.SalesOrderDetail e Production.Product, de forma a obter a soma total de produtos (OrderQty) por ProductID e OrderDate.

```sql
"select production.ProductID, first(production.Name) as Prodct_Names, SalesHeader.OrderDate, sum(SalesDetail.OrderQty) as Order_Qty
 from production LEFT JOIN SalesDetail ON production.ProductID = SalesDetail.ProductID 
 LEFT JOIN SalesHeader ON SalesDetail.SalesOrderID = SalesHeader.SalesOrderID 
 group by production.ProductID, SalesHeader.OrderDate order by Order_Qty desc"
```

![GitHub Logo](/Images/Q4.png)
5.	Escreva uma query mostrando os campos SalesOrderID, OrderDate e TotalDue da tabela Sales.SalesOrderHeader. Obtenha apenas as linhas onde a ordem tenha sido feita durante o mês de setembro/2011 e o total devido esteja acima de 1.000. Ordene pelo total devido decrescente.
```sql
"select SalesOrderID, OrderDate, TotalDue
 from SalesHeader where OrderDate >= '2011-09-01' and OrderDate <= '2011-09-30' and TotalDue > 1000"
```
![GitHub Logo](/Images/Q5.png)

### Merge de todas as bases
    
Uma opção é juntar todas as bases. Para isso, considerei mais razoável fazer left join.
Contudo, como algumas bases possuem calunas com mesmo nome, achei melhor colocar um sufixo nos nomes das colunas.
    
![GitHub Logo](/Images/sufixos1.png)

![GitHub Logo](/Images/sufixos2.png)

```sql
"select production.*, SpecialOfferProduct.*, SalesDetail.*, SalesHeader.*, SalesCustomer.*, person.*
 from production LEFT JOIN SpecialOfferProduct ON production.ProductID_production = SpecialOfferProduct.ProductID_SpecialOfferProduct 
 LEFT JOIN SalesDetail ON SpecialOfferProduct.SpecialOfferID_SpecialOfferProduct = SalesDetail.SalesOrderDetailID_SalesDetail 
 LEFT JOIN SalesHeader ON SalesDetail.SalesOrderID_SalesDetail = SalesHeader.SalesOrderID_SalesHeader 
 LEFT JOIN SalesCustomer ON SalesCustomer.CustomerID_SalesCustomer = SalesHeader.CustomerID_SalesHeader 
 LEFT JOIN person ON person.BusinessEntityID_person = SalesCustomer.CustomerID_SalesCustomer"
```


## Visualização dos dados

![GitHub Logo](/Images/qtd_vendida.png)

![GitHub Logo](/Images/vendas_reais.png)

![GitHub Logo](/Images/sexo.png)

![GitHub Logo](/Images/sufixos2.png)

![GitHub Logo](/Images/sufixos2.png)

![GitHub Logo](/Images/sufixos2.png)
