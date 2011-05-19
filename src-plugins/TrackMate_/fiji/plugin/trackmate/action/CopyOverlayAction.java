package fiji.plugin.trackmate.action;

import ij.ImagePlus;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.ImageIcon;
import javax.swing.JFrame;

import fiji.plugin.trackmate.TrackMateModelInterface;
import fiji.plugin.trackmate.gui.DisplayerPanel;
import fiji.plugin.trackmate.gui.ImagePlusChooser;
import fiji.plugin.trackmate.util.GUIUtils;
import fiji.plugin.trackmate.visualization.SpotDisplayer;

public class CopyOverlayAction extends AbstractTMAction {

	private static final ImageIcon COPY_OVERLAY_ICON = new ImageIcon(DisplayerPanel.class.getResource("images/page_copy.png"));
	
	public CopyOverlayAction() {
		icon = COPY_OVERLAY_ICON;
	}	
	
	@Override
	public void execute(final TrackMateModelInterface model) {
		final ImagePlusChooser impChooser = new ImagePlusChooser();
		impChooser.setLocationRelativeTo(null);
		impChooser.setVisible(true);
		final ActionListener copyOverlayListener = new ActionListener() {			
			@Override
			public void actionPerformed(ActionEvent e) {
				if (e == impChooser.OK_BUTTON_PUSHED) {
					new Thread("TrackMate copying thread") {
						public void run() {
							// Instantiate displayer
							ImagePlus dest = impChooser.getSelectedImagePlus();
							impChooser.setVisible(false);
							SpotDisplayer newDisplayer;
							String title;
							if (null == dest) {
								newDisplayer = SpotDisplayer.instantiateDisplayer(SpotDisplayer.DisplayerType.THREEDVIEWER_DISPLAYER, model);
								title = "3D viewer overlay";
							} else {
								model.getSettings().imp = dest; // TODO TODO DANGER DANGER
								newDisplayer = SpotDisplayer.instantiateDisplayer(SpotDisplayer.DisplayerType.HYPERSTACK_DISPLAYER, model);
								title = dest.getShortTitle() + " ctrl";
							}
							newDisplayer.setSpots(model.getSpots());
							newDisplayer.setSpotsToShow(model.getSelectedSpots());
							newDisplayer.setTrackGraph(model.getTrackGraph());
							
							final DisplayerPanel newDisplayerPanel = new DisplayerPanel(model.getFeatureValues());
							JFrame newFrame = new JFrame(); 
							newFrame.getContentPane().add(newDisplayerPanel);
							newFrame.pack();
							newFrame.setTitle(title);
							newFrame.setSize(300, 470);
							newFrame.setLocationRelativeTo(null);
							newFrame.setVisible(true);
							
							GUIUtils.execLinkDisplayerToTuningGUI(newDisplayerPanel, newDisplayer, model);
						}
					}.start();
				} else {
					impChooser.removeActionListener(this);
					impChooser.setVisible(false);
				}
			}
		};
		impChooser.addActionListener(copyOverlayListener);
	}

	@Override
	public String getInfoText() {
		return "<html>" +
			"This action copies the overlay (spots and tracks) to a new existing ImageJ window <br> " +
			"or to a new 3D viewer window. This can be useful to have the tracks and spots <br> " +
			"displayed on a modified image. <br> " +
			"</html>" ;
	}
	
	@Override
	public String toString() {
		return "Copy overlay to...";
	}

}
