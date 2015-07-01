package esclient

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl.{index, _}
import constants.Constants
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.common.collect.ImmutableOpenMap

import scala.collection.mutable
import scala.concurrent.duration.Duration

/**
 * Created by IntelliJ IDEA.
 * User: archit.pandey
 * Date: 6/26/2015
 * Time: 6:16 PM
 */
class _index(client: ElasticClient) extends ElasticCommand with Constants {

  def exists(indexName: String) : Boolean = {
    val existsIndex = client.execute(
      index exists indexName
    ).await
    existsIndex.isExists
  }

  def add(indexName: String) = {
    client.execute(
      create index indexName
    ).await
  }

  def getList : mutable.MutableList[String]= {

    val indexList : mutable.MutableList[String] = mutable.MutableList.empty[String]

    val allIndices : ImmutableOpenMap[String, IndexMetaData]  = client.admin.cluster.prepareState.execute.actionGet.getState.getMetaData.indices
    val it: java.util.Iterator[String] = allIndices.keysIt()
    while(it.hasNext)
    {
      indexList += it.next
    }
    indexList
  }

  def openIx(indexName: String) = {
    client.execute(
      open index indexName
    )
  }

  def closeIx(indexName: String) = {
    client.execute(
        close index indexName
    ).await(Duration.Inf)
  }

  def findBackupIndicesToBuild(aliases: mutable.Map[String, mutable.MutableList[String]]): scala.collection.Set[_] =
  {
    val indToBuild = for (alias <- aliases.keySet; if alias.startsWith(aliasPrefix)) yield
    {
      if (aliases.get(alias).size > 1)
      {
        throw new RuntimeException("Multiple indexes on the same alias ")
      }
      val indexName = aliases.get(alias).get.head
      if(indexName.startsWith(backupIndexPrefix))
        None
      else
      {
        val backupIndex = backupIndexPrefix + indexName
        val existsIndex:Boolean = _index.this exists backupIndex
        existsIndex match
        {
          case false =>
            println("Backup index not found " + backupIndex + " = " + existsIndex)
            backupIndex
          case true => println("Index exists - " + indexName)
            None
        }
      }
    }
    indToBuild
  }
}
