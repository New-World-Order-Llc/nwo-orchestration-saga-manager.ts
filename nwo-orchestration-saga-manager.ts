import { SagaEnvelopeSchema } from "beast-contracts/orchestration";
import { publishEvent } from "../data/EventPublisher";

export class OrchestrationSagaManager {
  constructor(sagas) {
    this.sagas = sagas; // { sagaName: { steps: [...], compensations: [...] } }
  }

  run(sagaEnvelope) {
    const valid = SagaEnvelopeSchema.safeParse(sagaEnvelope);
    if (!valid.success) {
      const errorPacket = {
        id: crypto.randomUUID(),
        envelope: sagaEnvelope,
        reason: valid.error.message,
        rejectedAt: new Date().toISOString()
      };

      publishEvent("orchestration.saga.rejected", errorPacket);
      throw new Error("Invalid saga envelope");
    }

    const { sagaName, initialContext } = valid.data;
    const saga = this.sagas[sagaName];

    if (!saga) {
      throw new Error(`Undefined saga: ${sagaName}`);
    }

    let context = initialContext;
    const executedSteps = [];

    try {
      for (const stepFn of saga.steps) {
        context = stepFn(context);
        executedSteps.push(stepFn);

        publishEvent("orchestration.saga.step", {
          id: crypto.randomUUID(),
          sagaName,
          context,
          executedAt: new Date().toISOString()
        });
      }

      const completionPacket = {
        id: crypto.randomUUID(),
        sagaName,
        finalContext: context,
        completedAt: new Date().toISOString()
      };

      publishEvent("orchestration.saga.completed", completionPacket);
      return context;

    } catch (error) {
      for (const compensationFn of saga.compensations.reverse()) {
        compensationFn(context);

        publishEvent("orchestration.saga.compensation", {
          id: crypto.randomUUID(),
          sagaName,
          context,
          compensatedAt: new Date().toISOString()
        });
      }

      publishEvent("orchestration.saga.failed", {
        id: crypto.randomUUID(),
        sagaName,
        reason: error.message,
        failedAt: new Date().toISOString()
      });

      throw new Error(`Saga failed: ${error.message}`);
    }
  }
}
