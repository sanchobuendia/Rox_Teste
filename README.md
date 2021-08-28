# Rox_Teste

### O problema 
    O presente problema se refere aos dados de uma empresa que produz bicicletas. 
    
    O objetivo deste desafio é compreender os seus conhecimentos e experiência analisando os seguintes aspectos:
    
    1. Fazer a modelagem conceitual dos dados;
    2. Criação da infraestrutura necessária;
    3. Criação de todos os artefatos necessários para carregar os arquivos para o banco criado;
    4. Desenvolvimento de SCRIPT para análise de dados;
    5. (opcional) Criar um relatório em qualquer ferramenta de visualização de dados.
    
Os seguintes arquivos devem ser importados (ETL) para o banco de dados de sua escolha: 

    • Sales.SpecialOfferProduct.csv
    • Production.Product.csv
    • Sales.SalesOrderHeader.csv
    • Sales.Customer.csv
    • Person.Person.csv
    • Sales.SalesOrderDetail.csv


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







