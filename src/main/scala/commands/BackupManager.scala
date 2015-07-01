package commands

import java.util.concurrent.TimeoutException

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import constants.{Constants, SnapShot}
import exceptions.{ElasticSearchOperationTimeoutException, InvalidRepositoryException, NoSnapShotFoundException}
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.snapshots.SnapshotInfo

import scala.concurrent.duration.{Duration, DurationConversions}

/**
 * Created by IntelliJ IDEA.
 * User: archit.pandey
 * Date: 6/30/2015
 * Time: 2:51 PM
 */
class BackupManager(client: ElasticClient) extends Constants {

  val aliasManager = new AliasManager(client)

  def restoreOnlineIx(repositoryName: String) =
  {
    //TODO: Validate all index aliases are intact - fail if not
    val reposIt = client.admin.cluster.prepareGetRepositories(repositoryName).execute.get.iterator()
    val reposMetaData = if (reposIt.hasNext)
    {
      reposIt.next
    } else
    {
      null
    }

    if (reposMetaData == null)
    {
      throw new InvalidRepositoryException("Repository does not exist - " + repositoryName)
    }
    //find snapshot to restore backup from
    val maxSnapShotInfo: SnapshotInfo = getLatestSlice(repositoryName)
    //restore backup of indexes from maxSlice
    if (maxSnapShotInfo == null)
    {
      throw new NoSnapShotFoundException("No snapshot was found - " + repositoryName)
    }
    val maxSlice = SnapShot(maxSnapShotInfo.name)

    val snapshotRestore: RestoreSnapshotResponse = client.execute(
      (restore snapshot maxSnapShotInfo.name from repositoryName).renamePattern("(.+)").renameReplacement("index_"+maxSlice+"_$1").waitForCompletion(true)
    ).await(Duration.Inf)
    val restoreStatus: RestStatus = snapshotRestore.status()
    println("Backup restore status : " + restoreStatus)

    //switch to backup index
    aliasManager.activateBackupAlias()

    //TODO: drop the previous index
    //drop old indexes
    //aliasManager.activateOnlineAlias()
  }

  def getLatestSlice(repositoryName: String): SnapshotInfo =
  {
    var maxSnapShotInfo: SnapshotInfo = null
    var maxSlice: SnapShot = null
    val snapShotIterator = client.admin.cluster().prepareGetSnapshots(repositoryName).get().getSnapshots.iterator()
    while (snapShotIterator.hasNext)
    {
      val snapshotInfo = snapShotIterator.next
      if (snapshotNamePattern.matcher(snapshotInfo.name).matches())
      {
        val snapShotSlice = try
        {
          SnapShot(snapshotInfo.name)
        } catch
          {
            case ex: Throwable =>
              ex.printStackTrace()
              println("Ignoring bad snapshot name")
              None
          }

        snapShotSlice match
        {
          case None => //do nothing
          case x: SnapShot =>
            maxSlice = if (x.after(maxSlice))
            {
              maxSnapShotInfo = snapshotInfo
              x
            } else
            {
              maxSlice
            }
        }
      }
    }
    maxSnapShotInfo
  }
}
