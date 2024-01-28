package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.statemachine.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.SetTTLState;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class SetTTLProcedure extends StateMachineProcedure<ConfigNodeProcedureEnv, SetTTLState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SetTTLProcedure.class);

  private SetTTLPlan plan;

  public SetTTLProcedure() {
    super();
  }

  public SetTTLProcedure(SetTTLPlan plan) {
    this.plan = plan;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, SetTTLState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case SET_CONFIGNODE_TTL:
          setConfigNodeTTL(env);
          setNextState(SetTTLState.UPDATE_DATANODE_CACHE);
          return Flow.HAS_MORE_STATE;
        case UPDATE_DATANODE_CACHE:
          updateDataNodeTTL(env);
          return Flow.NO_MORE_STATE;
        default:
          return Flow.NO_MORE_STATE;
      }
    } finally {
      LOGGER.info("SetTTL-[{}] costs {}ms", state, (System.currentTimeMillis() - startTime));
    }
  }

  private void setConfigNodeTTL(ConfigNodeProcedureEnv env) {
    TSStatus res;
    try {
      res = env.getConfigManager().getConsensusManager().write(this.plan);
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
    }
    if (res.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.info("Failed to execute plan {} because {}", plan, res.message);
      setFailure(new ProcedureException(new IoTDBException(res.message, res.code)));
    }
  }

  private void updateDataNodeTTL(ConfigNodeProcedureEnv env) {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    AsyncClientHandler<TSetTTLReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.SET_TTL,
            new TSetTTLReq(
                Collections.singletonList(String.join(".", plan.getDatabasePathPattern())),
                plan.getTTL()),
            dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (TSStatus status : statusMap.values()) {
      // all dataNodes must clear the related schemaengine cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error("Failed to update ttl cache of dataNode.");
        setFailure(
            new ProcedureException(new MetadataException("Update dataNode ttl cache failed")));
        return;
      }
    }
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, SetTTLState setTTLState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected SetTTLState getState(int stateId) {
    return SetTTLState.values()[stateId];
  }

  @Override
  protected int getStateId(SetTTLState setTTLState) {
    return setTTLState.ordinal();
  }

  @Override
  protected SetTTLState getInitialState() {
    return SetTTLState.SET_CONFIGNODE_TTL;
  }
}
