package commands

import com.sksamuel.elastic4s.ElasticClient
import constants.Constants
import esclient._
import exceptions.BadIndexAliasException

import scala.collection.mutable

class AliasManager(client: ElasticClient) extends Constants {

  val _index = new _index(client)
  val _aliases = new _aliases(client)

  def activateBackupAlias() =
  {
    val aliases: mutable.Map[String, mutable.MutableList[String]] = _aliases fetchAll;

    for (aliasName <- aliases.keySet
         if aliasName.startsWith(aliasPrefix))
    {
      if (aliases.get(aliasName).size > 1)
      {
        throw new BadIndexAliasException("Multiple indexes on the same alias - " + aliasName)
      }
      val indexName = aliases.get(aliasName).get.head
      if (!indexName.startsWith(backupIndexPrefix))
      {
        val backupIndex = backupIndexPrefix + indexName
        println("Creating backup alias [" + aliasName + "] -> " + backupIndex)

        _aliases removeAlias(aliasName, indexName)
        _aliases addAlias(aliasName, backupIndex)
      }
      else {
        val onlineIndex = indexName.substring(backupIndexPrefix.length)
        _index closeIx onlineIndex
        println("Index closed - " + onlineIndex)
      }
    }
  }

  def activateOnlineAlias() = {
    val aliases: mutable.Map[String, mutable.MutableList[String]] = _aliases fetchAll;
    for (aliasName <- aliases.keySet
         if aliasName.startsWith(aliasPrefix))
    {
      if (aliases.get(aliasName).size > 1)
      {
        throw new BadIndexAliasException("Multiple indexes on the same alias - " + aliasName)
      }

      val indexName = aliases.get(aliasName).get.head

      if(indexName.startsWith(backupIndexPrefix))
      {
        val onlineIndex = indexName.substring(backupIndexPrefix.length)
        println("Creating online alias [" + aliasName + "] -> " + onlineIndex)

        _index openIx onlineIndex

        _aliases removeAlias (aliasName, indexName)
        _aliases addAlias(aliasName, onlineIndex)

        //TODO: Drop the backup index : indexName
      }
      else
      {
        println("Alias mapped to not-backup type index - " + aliasName + "=>" + indexName)
      }
    }
  }

  def createBackupAliases() = {
    for(indexName <- _index getList;
        if !indexName.startsWith(backupIndexPrefix))
    {
      val aliasName : String = aliasPrefix + indexName
      val existsIndex = _aliases exists aliasName
      if(!existsIndex)
      {
        _aliases addAlias(aliasName, indexName)
        println("Added alias [ " + aliasName + "] on " + indexName)
      }
      else {
        println("Alias already exists [ " + aliasName + "]")
      }
    }
  }

}
