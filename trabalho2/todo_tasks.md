# Fase 1: Infraestrutura e Comunicação PyRO

[x] 1- Inicie o Servidor de Nomes do PyRO em um terminal separado usando o comando pyro5-ns

[x] 2- Defina portas e IDs fixos para os 4 processos (ex: portas 9001-9004 e objectIds raft.node1 a node4)

[x] 3- Crie a classe do nó Raft utilizando o decorador @expose para permitir acesso remoto aos seus métodos

[x] 4- Configure URIs hardcoded dentro do código de cada nó no formato PYRO:objectId@localhost:porta para que os processos se comuniquem diretamente entre si

[x] 5- Inicialize os 4 processos no estado inicial de "Seguidor"


# Fase 2: Lógica de Eleição

[x] 1- Implemente um temporizador de eleição aleatório em cada nó para evitar empates de candidatos ao mesmo tempo

[x] 2- Crie a transição para Candidato, incrementando o "termo atual" e solicitando votos aos outros nós quando o temporizador expirar

[x] 3- Implemente a lógica de votação, garantindo que um nó só vote em um candidato se o termo for atual e o log não estiver obsoleto

[x] 4- Configure o registro do Líder no Servidor de Nomes com o nome "Líder", lembrando de configurar o PyRO para sobrescrever entradas anteriores

[ ] 5- Implemente o envio de heartbeats (mensagens periódicas vazias) do Líder para os Seguidores para manter a autoridade

# Fase 3: Replicação de Log e Cliente

[x] 1- Desenvolva o processo Cliente para pesquisar a URI do "Líder" no Servidor de Nomes e enviar comandos

[x] 2- Crie a recepção de comandos no Líder, anexando o comando ao log local como uma entrada não efetivada (uncommitted)

[ ] 3- Implemente a replicação paralela, onde o Líder envia a nova entrada de log para todos os Seguidores via RPC

[ ] 4- Estabeleça a regra do Quórum, onde o Líder só efetiva (commit) a entrada após receber confirmação da maioria (neste caso, 3 dos 4 processos)

[ ] 5- Envie a ordem de commit do Líder para os Seguidores assim que a maioria confirmar a replicação

# Fase 4: Testes de Resiliência

[ ] 1- Simule uma falha encerrando o processo do Líder e verifique se os Seguidores iniciam uma nova eleição automaticamente

[ ] 2- Confirme a consistência verificando se, após a reeleição, o novo Líder e os Seguidores mantêm exatamente a mesma sequência e ordem de comandos no log



# Observações

## 3. O Quórum Específico para 4 Processos (Valor 15)
O documento define que uma entrada só é efetivada (committed) se a maioria dos seguidores confirmar

  * Cálculo da Maioria: Em um sistema com 4 processos, a maioria é composta por 3 processos (o líder + 2 seguidores)
  * Ordem de Confirmação: O líder primeiro anexa ao seu próprio log (como uncommitted), envia aos seguidores e, somente após receber o "OK" de pelo menos 2 deles, ele faz o commit local e avisa os outros para fazerem o mesmo