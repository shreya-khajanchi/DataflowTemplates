/*
 * Copyright (C) 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.SinkDialect;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SchemaUtilsTest {

  @Test
  public void testDAGConstructionSimple() {
    // Parent -> Child
    DataGeneratorTable parent =
        DataGeneratorTable.builder()
            .name("Parent")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .isRoot(false) // Default false, should be set to true
            .childTables(ImmutableList.of())
            .build();

    DataGeneratorTable child =
        DataGeneratorTable.builder()
            .name("Child")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_parent")
                        .keyColumns(ImmutableList.of("parentId"))
                        .referencedTable("Parent")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .isRoot(true) // Should be set to false
            .childTables(ImmutableList.of())
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .dialect(SinkDialect.GOOGLE_STANDARD_SQL)
            .tables(ImmutableMap.of("Parent", parent, "Child", child))
            .build();

    DataGeneratorSchema dagSchema = SchemaUtils.setSchemaDAG(schema);

    DataGeneratorTable newParent = dagSchema.tables().get("Parent");
    DataGeneratorTable newChild = dagSchema.tables().get("Child");

    assertTrue(newParent.isRoot());
    assertFalse(newChild.isRoot());
    assertEquals(1, newParent.childTables().size());
    assertEquals("Child", newParent.childTables().get(0));
    assertEquals(0, newChild.childTables().size());
  }

  @Test
  public void testDAGConstructionMultiParentChain() {
    // P1 (10 QPS) -> P2 (100 QPS) -> Child
    DataGeneratorTable p1 =
        DataGeneratorTable.builder()
            .name("P1")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .isRoot(false)
            .build();

    DataGeneratorTable p2 =
        DataGeneratorTable.builder()
            .name("P2")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(100)
            .isRoot(false)
            .build();

    DataGeneratorTable child =
        DataGeneratorTable.builder()
            .name("Child")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_p1")
                        .keyColumns(ImmutableList.of("p1Id"))
                        .referencedTable("P1")
                        .referencedColumns(ImmutableList.of("id"))
                        .build(),
                    DataGeneratorForeignKey.builder()
                        .name("fk_p2")
                        .keyColumns(ImmutableList.of("p2Id"))
                        .referencedTable("P2")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .uniqueKeys(ImmutableList.of())
            .insertQps(200)
            .isRoot(false)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .dialect(SinkDialect.GOOGLE_STANDARD_SQL)
            .tables(ImmutableMap.of("P1", p1, "P2", p2, "Child", child))
            .build();

    DataGeneratorSchema dagSchema = SchemaUtils.setSchemaDAG(schema);

    DataGeneratorTable newP1 = dagSchema.tables().get("P1");
    DataGeneratorTable newP2 = dagSchema.tables().get("P2");
    DataGeneratorTable newChild = dagSchema.tables().get("Child");

    assertTrue(newP1.isRoot());
    assertFalse(newP2.isRoot());
    assertFalse(newChild.isRoot());

    // P1 should have P2
    assertEquals(1, newP1.childTables().size());
    assertEquals("P2", newP1.childTables().get(0));

    // P2 should have Child
    assertEquals(1, newP2.childTables().size());
    assertEquals("Child", newP2.childTables().get(0));

    assertEquals(0, newChild.childTables().size());
  }

  @Test
  public void testDAGConstructionInterleaving() {
    // InterleavedParent -> Child (interleaved)
    // OtherParent (1 QPS) -> Child (FK)
    // Interleaving should take precedence.

    DataGeneratorTable interleavedParent =
        DataGeneratorTable.builder()
            .name("InterleavedParent")
            .insertQps(100)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();
    DataGeneratorTable otherParent =
        DataGeneratorTable.builder()
            .name("OtherParent")
            .insertQps(1)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    DataGeneratorTable child =
        DataGeneratorTable.builder()
            .name("Child")
            .interleavedInTable("InterleavedParent")
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_other")
                        .keyColumns(ImmutableList.of("otherId"))
                        .referencedTable("OtherParent")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .insertQps(10)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .dialect(SinkDialect.GOOGLE_STANDARD_SQL)
            .tables(
                ImmutableMap.of(
                    "InterleavedParent",
                    interleavedParent,
                    "OtherParent",
                    otherParent,
                    "Child",
                    child))
            .build();

    DataGeneratorSchema dagSchema = SchemaUtils.setSchemaDAG(schema);

    assertFalse(dagSchema.tables().get("InterleavedParent").isRoot());
    assertTrue(dagSchema.tables().get("OtherParent").isRoot());
    assertFalse(dagSchema.tables().get("Child").isRoot());

    assertEquals(1, dagSchema.tables().get("OtherParent").childTables().size());
    assertEquals("InterleavedParent", dagSchema.tables().get("OtherParent").childTables().get(0));
    assertEquals(1, dagSchema.tables().get("InterleavedParent").childTables().size());
    assertEquals("Child", dagSchema.tables().get("InterleavedParent").childTables().get(0));
  }

  @Test
  public void testDAGConstructionGrandChild() {
    // P1 -> P2 -> C1 -> GC1
    // P2 -> C2
    // P1 (10), P2 (100), C1 (20), C2 (30), GC1 (40)
    // C1 now has parents P1 and P2

    DataGeneratorTable p1 =
        DataGeneratorTable.builder()
            .name("P1")
            .insertQps(10)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();
    DataGeneratorTable p2 =
        DataGeneratorTable.builder()
            .name("P2")
            .insertQps(100)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();
    DataGeneratorTable c1 =
        DataGeneratorTable.builder()
            .name("C1")
            .insertQps(20)
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_p1")
                        .keyColumns(ImmutableList.of("p1Id"))
                        .referencedTable("P1")
                        .referencedColumns(ImmutableList.of("id"))
                        .build(),
                    DataGeneratorForeignKey.builder()
                        .name("fk_p2_c1")
                        .keyColumns(ImmutableList.of("p2Id"))
                        .referencedTable("P2")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();
    DataGeneratorTable c2 =
        DataGeneratorTable.builder()
            .name("C2")
            .insertQps(30)
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_p2")
                        .keyColumns(ImmutableList.of("p2Id"))
                        .referencedTable("P2")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();
    DataGeneratorTable gc1 =
        DataGeneratorTable.builder()
            .name("GC1")
            .insertQps(40)
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_c1")
                        .keyColumns(ImmutableList.of("c1Id"))
                        .referencedTable("C1")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .dialect(SinkDialect.GOOGLE_STANDARD_SQL)
            .tables(ImmutableMap.of("P1", p1, "P2", p2, "C1", c1, "C2", c2, "GC1", gc1))
            .build();

    DataGeneratorSchema dagSchema = SchemaUtils.setSchemaDAG(schema);

    assertTrue(dagSchema.tables().get("P1").isRoot());
    assertFalse(dagSchema.tables().get("P2").isRoot()); // P2 is now a sequence child of P1 for C1
    assertFalse(dagSchema.tables().get("C1").isRoot());
    assertFalse(dagSchema.tables().get("C2").isRoot());
    assertFalse(dagSchema.tables().get("GC1").isRoot());

    assertEquals(1, dagSchema.tables().get("P1").childTables().size());
    assertEquals("P2", dagSchema.tables().get("P1").childTables().get(0)); // P1 -> P2 sequence

    assertEquals(2, dagSchema.tables().get("P2").childTables().size());
    assertTrue(dagSchema.tables().get("P2").childTables().contains("C1")); // P2 -> C1 sequence
    assertTrue(dagSchema.tables().get("P2").childTables().contains("C2")); // P2 -> C2 direct

    assertEquals(1, dagSchema.tables().get("C1").childTables().size());
    assertEquals("GC1", dagSchema.tables().get("C1").childTables().get(0));
    assertEquals(0, dagSchema.tables().get("C2").childTables().size());
    assertEquals(0, dagSchema.tables().get("GC1").childTables().size());
  }

  /**
   * Bug A — sibling roots with a shared grandchild across lineages.
   *
   * <p>Schema shape:
   *
   * <pre>
   *   R1 (root, qps=10)        R2 (root, qps=10)
   *        |                         |
   *        X (fk -> R1)              Y (fk -> R2)
   *         \                       /
   *          +---- Z (fk -> X, fk -> Y) ----+
   * </pre>
   *
   * <p>R1 and R2 are two independent sibling roots with no FK between them. X is R1's only child,
   * Y is R2's only child, and Z has FKs to BOTH X and Y (but not to R1 or R2 directly).
   *
   * <p>The current {@code setSchemaDAG} runs the QPS-sorted chaining on Z's parents and produces
   * the DAG:
   *
   * <pre>
   *   R1 -> X
   *   R2 -> Y
   *   X  -> Y    (SYNTHETIC edge from Z's QPS chain; no real FK relationship)
   *   Y  -> Z
   * </pre>
   *
   * <p>At runtime, {@code BatchAndWrite.generateChildRow} resolves every FK by looking the target
   * table up in {@code ancestorRows}, which is populated only along the recursion path from a
   * single ticking root. Walk each root's cascade:
   *
   * <ul>
   *   <li>R1 ticks: path R1 -> X -> Y -> Z. At Y, {@code ancestorRows = {R1, X}}. Y's FK target R2
   *       is NOT in the ancestor lineage, so Y's row is dropped (unresolvableFkChildrenDropped
   *       metric increments). Z is never reached.
   *   <li>R2 ticks: path R2 -> Y -> Z. At Z, {@code ancestorRows = {R2, Y}}. Z's FK target X is
   *       NOT in the ancestor lineage, so Z's row is dropped.
   * </ul>
   *
   * <p>Net: Z gets zero rows under any tick, and Y gets only half its expected cascade volume
   * (only R2's path succeeds; R1's path silently drops Y mid-chain). This failure mode cannot be
   * fixed by tweaking the QPS sort — the DAG stored in {@code childTables} is a single-parent
   * tree, so no linearization of the parents can place BOTH R1's and R2's lineages upstream of Z
   * simultaneously.
   *
   * <p>Options to fix:
   *
   * <ol>
   *   <li>Reject this shape at load time in {@code validateNoDuplicateFkTargets} — e.g., fail if
   *       any child table's FK targets live on sibling root chains that don't meet upstream.
   *   <li>Redesign cascade dispatch to resolve cross-lineage FKs via state lookups rather than
   *       relying purely on the recursion-path {@code ancestorRows}.
   * </ol>
   *
   * <p>This test documents the current (buggy) DAG shape so the failure mode is visible; it does
   * NOT assert the "correct" shape because the single-parent DAG representation cannot express
   * it.
   */
  @Test
  public void testDAGConstructionSiblingRootsSharedGrandchild_BugA() {
    DataGeneratorTable r1 =
        DataGeneratorTable.builder()
            .name("R1")
            .insertQps(10)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();
    DataGeneratorTable r2 =
        DataGeneratorTable.builder()
            .name("R2")
            .insertQps(10)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();
    DataGeneratorTable x =
        DataGeneratorTable.builder()
            .name("X")
            .insertQps(5)
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_r1")
                        .keyColumns(ImmutableList.of("r1Id"))
                        .referencedTable("R1")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();
    DataGeneratorTable y =
        DataGeneratorTable.builder()
            .name("Y")
            .insertQps(7) // strictly less than X so QPS sort on Z's parents is deterministic
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_r2")
                        .keyColumns(ImmutableList.of("r2Id"))
                        .referencedTable("R2")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();
    DataGeneratorTable z =
        DataGeneratorTable.builder()
            .name("Z")
            .insertQps(3)
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_x")
                        .keyColumns(ImmutableList.of("xId"))
                        .referencedTable("X")
                        .referencedColumns(ImmutableList.of("id"))
                        .build(),
                    DataGeneratorForeignKey.builder()
                        .name("fk_y")
                        .keyColumns(ImmutableList.of("yId"))
                        .referencedTable("Y")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .dialect(SinkDialect.GOOGLE_STANDARD_SQL)
            .tables(ImmutableMap.of("R1", r1, "R2", r2, "X", x, "Y", y, "Z", z))
            .build();

    DataGeneratorSchema dagSchema = SchemaUtils.setSchemaDAG(schema);

    // Both sibling roots remain roots — the QPS sort only chains parents within a single child's
    // parents list, never across children. R1 and R2 never meet in the DAG.
    assertTrue(dagSchema.tables().get("R1").isRoot());
    assertTrue(dagSchema.tables().get("R2").isRoot());
    assertFalse(dagSchema.tables().get("X").isRoot());
    assertFalse(dagSchema.tables().get("Y").isRoot());
    assertFalse(dagSchema.tables().get("Z").isRoot());

    // R1 -> X (real FK edge, from X's own parents processing).
    assertEquals(ImmutableList.of("X"), dagSchema.tables().get("R1").childTables());

    // R2 -> Y (real FK edge, from Y's own parents processing).
    assertEquals(ImmutableList.of("Y"), dagSchema.tables().get("R2").childTables());

    // X -> Y is SYNTHETIC. Z's parents are [X, Y]; sorted ascending by QPS (X@5 < Y@7) the chain
    // is X -> Y -> Z, so Y appears as a child of X even though there is no real FK between
    // them. This is where the schema's two independent lineages collide.
    assertEquals(ImmutableList.of("Y"), dagSchema.tables().get("X").childTables());

    // Y -> Z (last link of Z's parent chain).
    assertEquals(ImmutableList.of("Z"), dagSchema.tables().get("Y").childTables());

    // Z has no children.
    assertEquals(ImmutableList.of(), dagSchema.tables().get("Z").childTables());

    // Key observation for Bug A: Y has TWO DAG parents now — R2 (real FK edge) and X (synthetic
    // chain edge). At runtime, ancestorRows only contains the single root-to-child path taken,
    // so neither root's cascade reaches Z with both of Z's FK targets resolved:
    //
    //   - R1 ticks: path R1 -> X -> Y -> Z. At Y, FK target R2 is missing from {R1, X}. Drop Y.
    //     Z never reached.
    //   - R2 ticks: path R2 -> Y -> Z. At Z, FK target X is missing from {R2, Y}. Drop Z.
    //
    // Either way, Z gets zero rows. Fixing this would require expressing a non-tree DAG for Z
    // (two incoming edges from X and Y, both reachable from the root lineage), which the
    // current single-parent childTables list cannot represent.
  }
}
