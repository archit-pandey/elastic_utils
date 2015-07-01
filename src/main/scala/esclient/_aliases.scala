package esclient

import com.sksamuel.elastic4s.{GetAliasDefinition, ElasticClient}
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest

import scala.collection.mutable

class _aliases(client: ElasticClient) extends ElasticCommand {

      def fetchAll : mutable.Map[String,  mutable.MutableList[String]] = {

        val aliasIndexMap = mutable.Map.empty[String, mutable.MutableList[String]]

        val indicesAliasResponse = client.admin.cluster.prepareState.execute.actionGet.getState.getMetaData.aliases
        val it = indicesAliasResponse.keysIt()
        while(it.hasNext)
        {
          val alias = it.next
          println(alias)
          val indicesMap = indicesAliasResponse.get(alias)

          val indexList : mutable.MutableList[String] = mutable.MutableList.empty[String]
          aliasIndexMap += (alias -> indexList)

          val indicesNames = indicesMap.keys().toArray
          for(vtype <- indicesNames)
            {
              println(vtype)
              indexList += vtype.toString
            }
        }
        aliasIndexMap
      }

      def addAlias(aliasName: String, indexName : String) ={
          client.execute(
              add alias aliasName on indexName
          ).await
      }

      def removeAlias(aliasName: String, indexName : String) ={
        client.execute(
          remove alias aliasName on indexName
        ).await
      }

      def exists(aliasName: String): Boolean = {
        val aliasDefn : GetAliasDefinition = getAlias(aliasName)
        val indices : Array[String] = aliasDefn.request.indices()
        indices != null && indices.length > 0
      }
}

