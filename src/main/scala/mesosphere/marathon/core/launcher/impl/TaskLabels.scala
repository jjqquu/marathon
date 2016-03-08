package mesosphere.marathon.core.launcher.impl

import mesosphere.marathon.core.task.Task
import org.apache.mesos.Protos.Label
import org.apache.mesos.{ Protos => MesosProtos }

import scala.collection.mutable

object TaskLabels {
  private[this] final val TASK_ID_LABEL = "marathon_task_id"

  def taskIdForResource(resource: MesosProtos.Resource): Option[Task.Id] = {
    if (!resource.hasReservation || !resource.getReservation.hasLabels) None
    else {
      import scala.collection.JavaConverters._
      val labels: Iterable[MesosProtos.Label] = resource.getReservation.getLabels.getLabelsList.asScala
      labels.find(_.getKey == TASK_ID_LABEL).map(label => Task.Id(label.getValue))
    }
  }

  def labelsForTask(task: Task): Map[String, String] = labelsForTask(task.taskId)
  def labelsForTask(taskId: Task.Id): Map[String, String] = Map(TASK_ID_LABEL -> taskId.idString)

  def mesosLabelsForTask(taskId: Task.Id): MesosProtos.Labels = mesosLabels(labelsForTask(taskId))

  private[this] def mesosLabels(labels: Map[String, String]): MesosProtos.Labels = {
    val labelsBuilder = MesosProtos.Labels.newBuilder()
    labels.foreach {
      case (k, v) =>
        labelsBuilder.addLabels(MesosProtos.Label.newBuilder().setKey(k).setValue(v))
    }
    labelsBuilder.build()
  }
}
