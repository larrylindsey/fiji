package edu.utexas.clm.fixmontage;

import ij.IJ;
import ij.plugin.PlugIn;
import ini.trakem2.Project;
import ini.trakem2.display.AreaList;
import ini.trakem2.display.Layer;
import ini.trakem2.display.Patch;
import ini.trakem2.display.VectorData;
import ini.trakem2.utils.AreaUtils;
import ini.trakem2.utils.M;
import ini.trakem2.utils.Utils;
import mpicbg.models.CoordinateTransformMesh;
import org.jfree.data.general.Series;

import java.awt.geom.AffineTransform;
import java.awt.geom.Area;
import java.awt.geom.PathIterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A PlugIn to fix montage boo-boos caused by using Photoshop or some other non-scientific
 * image-processing software.
 */
public class Fix_Montage implements PlugIn
{
    public void run(String s)
    {
        /*fixPhotoshopMontage("/nfs/data0/home/larry/Series/CLZBJ/CLZBJ current traces.xml",
                "/nfs/data0/home/larry/Series/CLZBJ/CLZBJ before retro elastic/CLZBJ elastic.xml",
                "/nfs/data0/home/larry/Series/CLZBJ/CLZBJ square one/CLZBJ rectify.xml",
                "/nfs/data0/home/larry/Series/CLZBJ/CLZBJ square one/CLZBJ raw one.xml");*/

        final Project pTraces, pAlignment, pRectify, pMontage;

        pTraces = Project.openFSProject(
                "/nfs/data0/home/larry/Series/CLZBJ/CLZBJ current traces.xml");
        pAlignment = Project.openFSProject(
                "/nfs/data0/home/larry/Series/CLZBJ/CLZBJ before retro elastic/CLZBJ elastic.xml");
        pRectify = Project.openFSProject(
                "/nfs/data0/home/larry/Series/CLZBJ/CLZBJ square one/CLZBJ rectify.xml");
        pMontage = Project.openFSProject(
                "/nfs/data0/home/larry/Series/CLZBJ/CLZBJ square one/CLZBJ raw one.xml");

        final FixMontage fix = new FixMontage();
        fix.setAlignmentProject(pAlignment);
        fix.setTracesProject(pTraces);
        fix.setRectifyProject(pRectify);
        fix.setMontageProject(pMontage);

        fix.fixProjects();
    }



    public static void applyInversePatchTransform(final VectorData data,
                                                  final Patch patch,
                                                  final Layer layer,
                                                  final List<Area> exclusions)
    {
        List<Area> useExclusions = exclusions == null ? new ArrayList<Area>() :
                exclusions;

        InvertedCoordinateTransform ict =
                new InvertedCoordinateTransform(
                        new CoordinateTransformMesh(
                                patch.getFullCoordinateTransform(),
                                32, // or more if you want to be more precise
                                patch.getOWidth(),
                                patch.getOHeight()));

        Area applyArea = patch.getArea();

        for (Area subArea : useExclusions)
        {
            applyArea.subtract(subArea);
        }

        try
        {
            data.apply(layer, applyArea, ict);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            IJ.log("Could not apply inverted transform to " + data);
        }
    }

    /**
     * Given a Patch list and a Layer, finds the geometrically largest Patch in the layer, which
     * it returns. All other Patches in the Layer are added to the list.
     *
     * @param layer the Layer in question
     * @param patches a List to store the Layer's Patches.
     * @return the largest Patch in the Layer
     */
    public static Patch splitPatches(final Layer layer, final List<Patch> patches)
    {
        final List<Patch> layerPatches = Utils.castCollection(
                layer.getDisplayables(Patch.class, true),
                Patch.class, true);
        Patch maxPatch = null;

        for (final Patch p : layerPatches)
        {
            if (maxPatch == null ||
                    p.getHeight() * p.getWidth() > maxPatch.getHeight() * maxPatch.getWidth())
            {
                maxPatch = p;
            }
        }

        layerPatches.remove(maxPatch);
        patches.addAll(layerPatches);

        return maxPatch;
    }

    public static List<Patch> getPatches(final Layer l)
    {
        return Utils.castCollection(
                l.getDisplayables(Patch.class, true),
                Patch.class, true);
    }

    public static Area getTransformedArea(final AreaList areaList, final Layer layer)
    {
        Area area = new Area(areaList.getArea(layer));
        {
            AffineTransform at = areaList.getAffineTransform();
            area.transform(at);
        }
        return area;
    }


    public static void mapAreaToPatches(final Map<String, Area> patchAreaMap,
                                        final List<Patch> rectifyRawPatches,
                                        final AreaList rectifyAreaList,
                                        final Layer rectifyLayer)
    {
//        ArrayList<Patch> tempList;

        for (Patch p : rectifyRawPatches)
        {
            IJ.log("Mapping area for patch " + p.getImageFilePath());

            InvertedCoordinateTransform ict =
                    new InvertedCoordinateTransform(
                            new CoordinateTransformMesh(
                                    p.getFullCoordinateTransform(),
                                    16, // or more if you want to be more precise
                                    p.getOWidth(),
                                    p.getOHeight()));
            Area modifyArea = new Area(getTransformedArea(rectifyAreaList, rectifyLayer));
            IJ.log("Getting shrunk patch area");
            Area shrunkPatchArea = shrinkArea(p.getArea());
            modifyArea.intersect(shrunkPatchArea);

/*
            tempList = new ArrayList<Patch>(rectifyRawPatches);
            tempList.remove(p);

            for (Patch otherPatch : tempList)
            {
                modifyArea.subtract(otherPatch.getArea());
            }
*/
            IJ.log("Applying inverse transform");

            M.apply(ict, AreaUtils.infiniteArea(), modifyArea);

            IJ.log("Mapping...");

            patchAreaMap.put(p.getImageFilePath(), modifyArea);
            //modifyList.remove(false);
        }
    }

    public static void reconstituteArea(final Map<String, Area> patchAreaMap,
                                                final Layer layer,
                                                AreaList areaList)
    {
        List<Patch> patches = getPatches(layer);
        Area finalArea = new Area();
        for (Patch p : patches)
        {
            Area area = new Area(patchAreaMap.get(p.getImageFilePath()));
            M.apply(p.getFullCoordinateTransform(), area, area);
            finalArea.add(area);
        }
        areaList.addArea(layer.getId(), finalArea);
    }

    public static void transformAreaList(final Layer currLayer,
                                         final Layer alignmentLayer,
                                         final Layer rectifyLayer,
                                         final Layer rawLayer)
    {
        try
        {


            ArrayList<Patch> rectifyRawPatches = new ArrayList<Patch>();

            Patch shopPatch = splitPatches(rectifyLayer, rectifyRawPatches);
            Patch alignmentPatch = getPatches(alignmentLayer).get(0);

            AreaList currAreaList = (AreaList)currLayer.getParent().findById(262);

            final AreaList alignmentAreaList  = new AreaList(alignmentLayer.getProject(), "copy", 0, 0);
            final AreaList rectifyAreaList = new AreaList(rectifyLayer.getProject(), "rectify copy", 0, 0);
            final AreaList origRawAreaList  = new AreaList(rawLayer.getProject(), "raw copy orig", 0, 0);
            final AreaList rawAreaList = new AreaList(rawLayer.getProject(), "raw copy", 0, 0);

            final HashMap<String, Area> patchAreaMap = new HashMap<String, Area>();

            alignmentAreaList.addArea(alignmentLayer.getId(), getTransformedArea(currAreaList, currLayer));

            alignmentLayer.getParent().add(alignmentAreaList);

            rectifyAreaList.addArea(rectifyLayer.getId(), getTransformedArea(currAreaList, currLayer));

            applyInversePatchTransform(rectifyAreaList, alignmentPatch, rectifyLayer, null);

            rectifyAreaList.apply(rectifyLayer, AreaUtils.infiniteArea(),
                    shopPatch.getFullCoordinateTransform());

            rectifyLayer.getParent().add(rectifyAreaList);

            //rawAreaList.addArea(rawLayer.getId(), getTransformedArea(rectifyAreaList, rectifyLayer));
            origRawAreaList.addArea(rawLayer.getId(), getTransformedArea(rectifyAreaList, rectifyLayer));

            IJ.log("mapping areas to patches");


            mapAreaToPatches(patchAreaMap, rectifyRawPatches, rectifyAreaList, rectifyLayer);

            IJ.log("reconstituting patches");

            reconstituteArea(patchAreaMap, rawLayer, rawAreaList);

            rawLayer.getParent().add(rawAreaList);
            rawLayer.getParent().add(origRawAreaList);
        }
        catch (Exception e)
        {
            IJ.log("Caught Exception: " + e);
            e.printStackTrace();
        }
    }

    public static void fixPhotoshopMontage(final String currentProjectPath,
                                           final String alignmentProjectPath,
                                           final String rectifyProjectPath,
                                           final String rawProjectPath)
    {
        int z = 76;
        Project currentProject = Project.openFSProject(currentProjectPath);
        Project alignmentProject = Project.openFSProject(alignmentProjectPath);
        Project rectifyProject = Project.openFSProject(rectifyProjectPath);
        Project rawProject = Project.openFSProject(rawProjectPath);

        Layer currLayer = currentProject.getRootLayerSet().getLayers().get(z);
        Layer alignmentLayer = alignmentProject.getRootLayerSet().getLayers().get(z);
        Layer rectifyLayer = rectifyProject.getRootLayerSet().getLayers().get(z);
        Layer rawLayer = rawProject.getRootLayerSet().getLayers().get(z);

        transformAreaList(currLayer, alignmentLayer, rectifyLayer, rawLayer);
    }

    public static Area shrinkArea(final Area areaIn)
    {
        Area area = new Area(areaIn);
        PathIterator pit = area.getPathIterator(null);
        int type;
        final double[] location = new double[6];
        final double[] ctr = new double[6];
        int count = 0;

        while (!pit.isDone())
        {
            type = pit.currentSegment(location);
            if (type == PathIterator.SEG_LINETO || type == PathIterator.SEG_MOVETO)
            {
                ++count;
                ctr[0] += location[0];
                ctr[1] += location[1];
            }
            pit.next();
        }

        ctr[0] /= (double)count;
        ctr[1] /= (double)count;

        area.transform(AffineTransform.getTranslateInstance(-ctr[0], -ctr[1]));
        area.transform(AffineTransform.getScaleInstance(.98, .98));
        area.transform(AffineTransform.getTranslateInstance(ctr[0], ctr[1]));

        return area;
    }

    public static void printArea(final String name, final Area area, final AffineTransform at)
    {
        PathIterator pit = area.getPathIterator(at);
        float[] location = new float[6];
        int type;

        for (int i = 0; i < 6; ++i)
        {
            location[i] = Float.NaN;
        }

        IJ.log(name + " = [");

        while (!pit.isDone())
        {
            String disp;

            type = pit.currentSegment(location);

            disp = "" + type;

            for (int i = 0; i < 6; ++i)
            {
                disp += " " + location[i];
                location[i] = Float.NaN;
            }

            pit.next();

            IJ.log(disp + ";'");
        }

        IJ.log("];");

    }

    public static void main(final String[] args) throws Exception
    {
        IJ.log("Started");
        fixPhotoshopMontage("/nfs/data0/home/larry/Series/CLZBJ/CLZBJ current traces.xml",
                "/nfs/data0/home/larry/Series/CLZBJ/CLZBJ before retro elastic/CLZBJ elastic.xml",
                "/nfs/data0/home/larry/Series/CLZBJ/CLZBJ square one/CLZBJ rectify all.xml",
                "/nfs/data0/home/larry/Series/CLZBJ/CLZBJ square one/CLZBJ raw.xml");
    }

}
