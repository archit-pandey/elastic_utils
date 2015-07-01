
import com.sksamuel.elastic4s.ElasticClient
import commands.{BackupManager, AliasManager}
import constants.Constants

object ESIxExecutor extends Constants {
  val client = ElasticClient.remote("127.0.0.1", 9300)

  var action = ACTIVATEONLINE

  def main(args: Array[String])
  {
    action = RESTOREBACKUP

    println("Setting mode - " + action)
    val aliasManager  = new AliasManager(client)
    val backupManager  = new BackupManager(client)


    //swap indices
    action match {
      case ACTIVATEBACKUP =>
        aliasManager.activateBackupAlias()
      case ACTIVATEONLINE =>
        aliasManager.activateOnlineAlias()
      case CREATEALIAS =>
        aliasManager.createBackupAliases()
      case RESTOREBACKUP =>
        backupManager.restoreOnlineIx("s3buys")
      case _ => throw new RuntimeException("Invalid input option - " + action)
    }
  }
}



