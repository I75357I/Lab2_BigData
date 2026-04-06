import logging

try:
    from pyspark.java_gateway import ensure_callback_server_started
    _CALLBACK_AVAILABLE = True
except ImportError:
    _CALLBACK_AVAILABLE = False


class SparkJobStageLogger:
    class Java:
        implements = ["org.apache.spark.scheduler.SparkListenerInterface"]

    def __init__(self, app_logger: logging.Logger):
        self._log = app_logger

    def onJobStart(self, jobStart):
        try:
            seq = jobStart.stageIds()
            try:
                stages = [int(seq.apply(i)) for i in range(seq.size())]
            except Exception:
                stages = str(seq)
            self._log.info(f"job {jobStart.jobId()} started, stages={stages}")
        except Exception as exc:
            self._log.warning(f"job start parse error: {exc}")

    def onJobEnd(self, jobEnd):
        try:
            result = str(jobEnd.jobResult())
            status = "success" if "JobSucceeded" in result else f"failed({result})"
            self._log.info(f"job {jobEnd.jobId()} finished, status={status}")
        except Exception as exc:
            self._log.warning(f"job end parse error: {exc}")

    def onStageSubmitted(self, stageSubmitted):
        try:
            si = stageSubmitted.stageInfo()
            self._log.info(f"stage {si.stageId()} submitted, name={si.name()!r}, tasks={si.numTasks()}")
        except Exception as exc:
            self._log.warning(f"stage submitted parse error: {exc}")

    def onStageCompleted(self, stageCompleted):
        try:
            si = stageCompleted.stageInfo()
            failed = si.failureReason()
            if failed.isDefined():
                status = f"failed: {str(failed.get())[:120]}"
            else:
                status = "done"
            self._log.info(f"stage {si.stageId()} completed, name={si.name()!r}, status={status}")
        except Exception as exc:
            self._log.warning(f"stage completed parse error: {exc}")

    def onTaskStart(self, e): pass
    def onTaskGettingResult(self, e): pass
    def onTaskEnd(self, e): pass
    def onEnvironmentUpdate(self, e): pass
    def onBlockManagerAdded(self, e): pass
    def onBlockManagerRemoved(self, e): pass
    def onUnpersistRDD(self, e): pass
    def onApplicationStart(self, e): pass
    def onApplicationEnd(self, e): pass
    def onExecutorMetricsUpdate(self, e): pass
    def onExecutorAdded(self, e): pass
    def onExecutorRemoved(self, e): pass
    def onBlockUpdated(self, e): pass
    def onOtherEvent(self, e): pass
    def onMiscellaneousProcessAdded(self, e): pass


def register_spark_listener(spark, logger: logging.Logger) -> None:
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("py4j.java_gateway").setLevel(logging.WARNING)
    logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)

    if not _CALLBACK_AVAILABLE:
        logger.warning("pyspark.java_gateway not available, skipping spark listener")
        return

    try:
        sc = spark.sparkContext
        ensure_callback_server_started(sc._gateway)
        listener = SparkJobStageLogger(logger)
        sc._jvm.org.apache.spark.SparkContext.getOrCreate().addSparkListener(listener)
        logger.info("spark listener registered")
    except Exception as exc:
        logger.warning(f"spark listener registration failed: {exc}")
