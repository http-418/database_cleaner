require 'database_cleaner/neo4j/base'
require 'neo4j-core'

module DatabaseCleaner
  module Neo4j
    class Truncation
      include ::DatabaseCleaner::Neo4j::Base
      
      def start
        if( @max_node_id.nil? ) 
          @max_node_id = current_node_max
        end
      end

      def clean
        ::Neo4j::Transaction.run do
          # Optional match with WHERE clause fails.
          session._query( "MATCH (n)-[r]-(b) " +
                          "WHERE id(n) > #{@max_node_id} " +
                          "DELETE n,r" )
          session._query( "MATCH (n) " +
                          "WHERE id(n) > #{@max_node_id} " +
                          "DELETE n" )
        end

      end

      def current_node_max
        ::Neo4j::Transaction.run do
          session.query( 'MATCH (n) ' +
                         'RETURN id(n) AS id ' +
                         'ORDER BY id(n) DESC ' +
                         'LIMIT 1' ).first.id
        end
      end
    end
  end
end
