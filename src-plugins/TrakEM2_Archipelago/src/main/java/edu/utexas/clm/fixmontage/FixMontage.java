package edu.utexas.clm.fixmontage;

import edu.utexas.clm.archipelago.Cluster;
import edu.utexas.clm.archipelago.ijsupport.bottle.AreaBottler;
import edu.utexas.clm.archipelago.ijsupport.bottle.LayerBottler;
import edu.utexas.clm.archipelago.ijsupport.bottle.PatchBottler;
import ij.IJ;
import ini.trakem2.Project;
import ini.trakem2.display.AreaList;
import ini.trakem2.display.Layer;
import ini.trakem2.display.LayerSet;
import ini.trakem2.display.Patch;
import ini.trakem2.display.Polyline;
import ini.trakem2.display.Profile;
import ini.trakem2.tree.ProjectThing;
import ini.trakem2.utils.Utils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 *
 */
public class FixMontage
{
    public static class NotReadyException extends RuntimeException
    {
        public NotReadyException()
        {
            super("Not ready yet!");
        }
    }

    private boolean ready;
    // Current traces
    private Project tracesProject;
    // Alignment over photoshop-montaged images
    private Project alignmentProject;
    // Alignment between photoshop montage and original patches
    private Project rectifyProject;
    // Montage over original patches
    private Project montageProject;

    private final ExecutorService service;


    public FixMontage()
    {
        ready = false;

        tracesProject = null;
        alignmentProject = null;
        rectifyProject = null;
        montageProject = null;


        if (Cluster.activeCluster())
        {
            IJ.log("Using cluster");
            service = Cluster.getCluster().getService(1);
            Cluster.getCluster().addBottler(new PatchBottler());
            Cluster.getCluster().addBottler(new LayerBottler());
            Cluster.getCluster().addBottler(new AreaBottler());
        }
        else
        {
            IJ.log("Using local thread pool");
            service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        }
    }

    private void setReady()
    {
        ready = tracesProject != null &&
                alignmentProject != null &&
                rectifyProject != null &&
                montageProject != null;
    }

    public void setTracesProject(final Project project)
    {
        tracesProject = project;

        setReady();
    }

    public void setAlignmentProject(final Project project)
    {
        alignmentProject = project;

        setReady();
    }

    public void setRectifyProject(final Project project)
    {
        rectifyProject = project;

        setReady();
    }

    public void setMontageProject(final Project project)
    {
        montageProject = project;

        setReady();
    }

    public static String patchIdentifierFile(final Patch p)
    {
        File f = new File(p.getImageFilePath());
        return f.getName();
    }

    public void fixProjects() throws NotReadyException
    {
        if (!ready)
        {
            throw new NotReadyException();
        }
        else
        {
            final HashMap<String, FixMontageLayerCallable> rawPatchMap =
                    new HashMap<String, FixMontageLayerCallable>();
            final HashMap<String, FixMontageLayerCallable> shopPatchMap =
                    new HashMap<String, FixMontageLayerCallable>();
            final ArrayList<FixMontageLayerCallable> callables =
                    new ArrayList<FixMontageLayerCallable>();
            final ArrayList<Future<FixMontageLayerResult>> futures =
                    new ArrayList<Future<FixMontageLayerResult>>();

            for (final Layer rawLayer : montageProject.getRootLayerSet().getLayers())
            {
                final FixMontageLayerCallable callable = new FixMontageLayerCallable();

                for (final Patch p : getPatches(rawLayer))
                {
                    callable.addMontagePatch(p);
                    rawPatchMap.put(patchIdentifierFile(p), callable);
                    IJ.log("Mapped callable " + callable.getId() + " to " + p.getImageFilePath());
                }

                callables.add(callable);
            }

            for (final Layer rectifyLayer : rectifyProject.getRootLayerSet().getLayers())
            {
                final ArrayList<Patch> rectifyRawPatches = new ArrayList<Patch>();
                final Patch maxPatch = splitPatches(rectifyLayer, rectifyRawPatches);
                FixMontageLayerCallable callable = null;
                for (final Patch p : rectifyRawPatches)
                {
                    if ((callable = rawPatchMap.get(patchIdentifierFile(p))) != null)
                    {
                        IJ.log("Found callable " + callable.getId() + " mapped to " + p.getImageFilePath());
                        break;
                    }
                }

                if (callable != null)
                {
                    callable.setRectifyPatches(rectifyRawPatches, maxPatch);
                    shopPatchMap.put(patchIdentifierFile(maxPatch), callable);
                    IJ.log("Mapped callable " + callable.getId() + " to " + maxPatch.getImageFilePath());
                }
            }

            for (int i = 0; i < alignmentProject.getRootLayerSet().size(); ++i)
            {
                final Layer alignmentLayer = alignmentProject.getRootLayerSet().getLayer(i);
                final Layer tracesLayer = tracesProject.getRootLayerSet().getLayer(i);

                final Patch alignmentPatch = getPatch(alignmentLayer);
                final FixMontageLayerCallable callable;

                if (alignmentPatch != null &&
                        ((callable = shopPatchMap.get(patchIdentifierFile(alignmentPatch))) != null))
                {
                    callable.setAlignmentPatch(alignmentPatch);
                    callable.setTracesPatch(getPatch(tracesLayer));
                }
            }

            for (final FixMontageLayerCallable callable : callables)
            {
                futures.add(service.submit(callable));
            }

            try
            {
                applyResults(futures);
            }
            catch (Exception e)
            {
                e.printStackTrace();
                IJ.error("Error while processing: " + e);
            }
        }
    }

    private void applyResults(final List<Future<FixMontageLayerResult>> futures)
            throws ExecutionException, InterruptedException
    {
        final List<AreaList> origAreaLists = getAreaLists(tracesProject);
        final List<Polyline> origPolylines = getPolylines(tracesProject);

        final HashMap<Long, AreaList> origIdFixedAreaListMap = new HashMap<Long, AreaList>();
        final HashMap<Long, Polyline> origIdFixedPolylineMap = new HashMap<Long, Polyline>();

        final HashMap<Long, AreaList> fixedIdOrigAreaListMap = new HashMap<Long, AreaList>();
        final HashMap<Long, Polyline> fixedIdOrigPolylineMap = new HashMap<Long, Polyline>();

        final HashSet<Long> fixedIdsToAdd = new HashSet<Long>();

        int count = 0;

        IJ.log("Creating new objects...");

        for (final AreaList origAreaList : origAreaLists)
        {
            final AreaList fixedAreaList = new AreaList(montageProject,
                    origAreaList.getTitle(), 0, 0);
            //fixedAreaList.setAlpha(origAreaList.getAlpha());
            //fixedAreaList.setColor(areaList.getColor());
            fixedAreaList.setVisible(origAreaList.isVisible());

            origIdFixedAreaListMap.put(origAreaList.getId(), fixedAreaList);
            fixedIdOrigAreaListMap.put(fixedAreaList.getId(), origAreaList);
        }

        for (final Polyline origPolyline : origPolylines)
        {
            final Polyline fixedPolyline = new Polyline(montageProject, origPolyline.getTitle());
            fixedPolyline.setAlpha(origPolyline.getAlpha());
            //fixedPolyline.setVisible(origPolyline.isVisible());
            //fixedPolyline.setColor(polyline.getColor());

            origIdFixedPolylineMap.put(origPolyline.getId(), fixedPolyline);
            fixedIdOrigPolylineMap.put(fixedPolyline.getId(), origPolyline);
        }

        for (final Future<FixMontageLayerResult> future : futures)
        {
            IJ.log("Waiting for results over Layer " + (count + 1) + " of " + futures.size());
            final FixMontageLayerResult result = future.get();
            if (result != null)
            {
                LayerSet rls = tracesProject.getRootLayerSet();
                long id = result.getTracesLayerId();
                Layer l = rls.getLayer(id);
                final List<Profile> profiles = getProfiles(l);
                final Layer montageLayer =
                        montageProject.getRootLayerSet().getLayer(result.getMontageLayerId());

                ++count;

                IJ.log("Fixed objects in layer " + count + " of " + futures.size() +
                        ", applying to new project");

                for (final long key : origIdFixedPolylineMap.keySet())
                {
                    if (result.apply(origIdFixedPolylineMap.get(key), key))
                    {
                        fixedIdsToAdd.add(origIdFixedPolylineMap.get(key).getId());
                    }
                }

                for (final long key : origIdFixedAreaListMap.keySet())
                {
                    if (result.apply(origIdFixedAreaListMap.get(key), key))
                    {
                        fixedIdsToAdd.add(origIdFixedAreaListMap.get(key).getId());
                    }
                }

                for (final Profile profile : profiles)
                {
                    result.insertProfile(montageLayer, profile);
                }
            }
        }

        IJ.log("Adding area lists to project...");
        for (final long origId : origIdFixedAreaListMap.keySet())
        {
            final AreaList montageAreaList = origIdFixedAreaListMap.get(origId);
            if (fixedIdsToAdd.contains(montageAreaList.getId()))
            {
                final AreaList origAreaList =
                        fixedIdOrigAreaListMap.get(montageAreaList.getId());
                final String name =
                        tracesProject.findProjectThing(origAreaList).getParent().getTitle();

                montageAreaList.setTitle(name);
                montageProject.getRootLayerSet().add(montageAreaList);

                montageAreaList.setColor(origAreaList.getColor());
                montageAreaList.setAlpha(origAreaList.getAlpha());
                montageAreaList.setVisible(origAreaList.isVisible());
            }

        }

        IJ.log("Adding Z-traces to project...");

        for (final long origId : origIdFixedPolylineMap.keySet())
        {
            final Polyline montagePolyline = origIdFixedPolylineMap.get(origId);
            if (fixedIdsToAdd.contains(montagePolyline.getId()))
            {
                final Polyline origPolyline =
                        fixedIdOrigPolylineMap.get(montagePolyline.getId());
                final String name =
                        tracesProject.findProjectThing(origPolyline).getParent().getTitle();

                montageProject.getRootLayerSet().add(montagePolyline);

                montagePolyline.setColor(origPolyline.getColor());
                montagePolyline.setAlpha(origPolyline.getAlpha());
                montagePolyline.setVisible(origPolyline.isVisible());
            }
        }

        IJ.log("OK. I think we're done");
    }



    public static List<Patch> getPatches(final Layer l)
    {
        return Utils.castCollection(
                l.getDisplayables(Patch.class, true),
                Patch.class, true);
    }

    public static List<AreaList> getAreaLists(final Project p)
    {
        return Utils.castCollection(
                p.getRootLayerSet().getZDisplayables(AreaList.class, true),
                AreaList.class, true);
    }

    public static List<Polyline> getPolylines(final Project p)
    {
        return Utils.castCollection(
                p.getRootLayerSet().getZDisplayables(Polyline.class, true),
                Polyline.class, true);
    }


    public static List<Profile> getProfiles(final Layer l)
    {
        return Utils.castCollection(
                l.getDisplayables(Profile.class, true),
                Profile.class, true);
    }

    public static Patch getPatch(final Layer l)
    {
        final List<Patch> patches = getPatches(l);
        return patches.size() == 0 ? null : patches.get(0);
    }

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

}
