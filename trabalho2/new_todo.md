# 1. Inicialização e Comunicação (PyRO)

[x] Subir o Name Server: Lembrem-se de rodar o pyro5-ns no terminal antes de iniciar a aplicação.  
[x] Criar o script do Cliente: Fazer um arquivo separado (cliente.py) que será responsável por enviar os comandos.  
[x] Busca dinâmica do Líder: No script do Cliente, usar Pyro5.api.locate_ns().lookup("leader") para achar com quem ele deve conversar.  

# 2. Ajustes na Eleição
[x] Registro Global no Name Server: No método __become_leader, o código atual registra a string "leader" apenas no daemon local. Para cumprir o requisito, vocês precisam localizar o Name Server e registrar o URI lá: 

```py
ns = Pyro5.api.locate_ns()
ns.register("leader", self.uri)
```

# 3. Replicação de Log (A Lógica do Líder)Como o seguidor já está pronto, o foco agora é 100% no comportamento do Líder.

[x] Viviane - Integrar o comando do cliente ao Log: O método append_log exposto atualmente só salva no self.buffer. O líder precisa pegar esse comando, transformá-lo em um LogEntry com o self.current_term, anexar ao seu próprio self.log e iniciar a replicação para os seguidores.

[x] Inicializar o Estado do Líder: Ao vencer a eleição (__become_leader), o líder precisa inicializar dois dicionários: nextIndex (próximo índice a enviar para cada seguidor) e matchIndex (índice mais alto replicado com sucesso para cada seguidor).

[x] Viviane - Enviar os dados reais no Heartbeat/Append: No __heartbeat_loop, o append_entries está enviando zeros e listas vazias hardcoded. O líder precisa enviar os valores reais usando as variáveis self.log, self.current_term, self.commit_index e o nextIndex de cada seguidor.

[x] Nicolas - A Regra da Maioria (Commit): O líder deve avaliar os retornos booleanos do append_entries que faz aos seguidores. Se houver sucesso, ele atualiza o nextIndex e o matchIndex do seguidor. Se a maioria dos nós tiver um matchIndex maior que o commit_index  atual do líder, o líder avança seu próprio commit_index (efetivando a entrada).

[x] Nicolas - Lidar com falhas de consistência (Backtracking): Se o seguidor retornar False no append_entries por inconsistência de log, o líder deve decrementar o nextIndex daquele seguidor específico e tentar enviar novamente na próxima iteração do loop.